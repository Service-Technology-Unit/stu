package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

/**
 * <p>Updates the Master floor list from the FD&C floor list.</p>
 */
public class MasterFloorUpdate implements SpringBatchJob {
	private static final String SRC_SQL = "SELECT b.UCDHS_Bldg_Num + '-' + f.Floor_Code AS ID, 2 AS [LEVEL], b.UCDHS_Bldg_Num AS PARENT, f.Floor_Name AS NAME, b.Official_Name + ', ' + f.Floor_Name AS FULL_NAME, b.Address AS ADDRESS, b.City AS CITY, b.State AS STATE, b.Zip AS ZIP, b.Country_Code AS COUNTRY, b.Longitude AS LONGITUDE, b.Latitude AS LATITUDE, f.Above_Ground_Inches AS ALTITUDE, b.Official_Name AS BUILDING, f.Floor_Name AS FLOOR, NULL AS ROOM, b.Google_Maps_URL AS GOOGLE_MAPS_URL, NULL AS DEPARTMENT FROM Floor f LEFT OUTER JOIN Building b ON b.Bldg_Key=f.Bldg_Key WHERE f.Floor_Code<>'XX' AND ISNULL(b.UCDHS_Bldg_Num, '')<>'' ORDER BY ID";
	private static final String MST_SQL = "SELECT ID, [LEVEL], PARENT, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, ALTITUDE, BUILDING, FLOOR, ROOM, GOOGLE_MAPS_URL, DEPARTMENT FROM LOCATION WHERE [LEVEL]=2 ORDER BY ID";
	private static final String INSERT_SQL = "INSERT INTO LOCATION (ID, LEVEL, PARENT, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, ALTITUDE, BUILDING, FLOOR, GOOGLE_MAPS_URL, CREATED_BY, CREATED_ON, CREATED_IP, UPDATED_BY, UPDATED_ON, UPDATED_IP, UPDATE_COUNT) VALUES(?, 2, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, getdate(), ?, 0)";
	private static final String UPDATE_SQL = "UPDATE LOCATION SET PARENT=?, NAME=?, FULL_NAME=?, ADDRESS=?, CITY=?, STATE=?, ZIP=?, COUNTRY=?, LONGITUDE=?, LATITUDE=?, ALTITUDE=?, BUILDING=?, FLOOR=?, GOOGLE_MAPS_URL=?, UPDATED_BY=?, UPDATED_ON=getdate(), UPDATED_IP=?, UPDATE_COUNT=UPDATE_COUNT+1 WHERE ID=?";
	private static final String DELETE_SQL = "DELETE FROM LOCATION WHERE ID=?";
	private static final String UPDATE_HISTORY_SQL = "INSERT INTO LOCATION_HISTORY (LOCATION_ID, COLUMN_NAME, OLD_VALUE, NEW_VALUE, CREATED_BY, CREATED_ON, CREATED_IP) VALUES (?, ?, ?, ?, ?, getdate(), ?)";
	private static final String USER_ID = System.getProperty("user.name");
	private final Log log = LogFactory.getLog(getClass());
	private String localAddress = null;
	private Connection srcConn = null;
	private Statement srcStmt = null;
	private ResultSet srcRs = null;
	private Connection mstConn = null;
	private Statement mstStmt = null;
	private ResultSet mstRs = null;
	private DataSource mstDataSource = null;
	private DataSource srcDataSource = null;
	private Up2DateService up2dateService = null;
	private String publisherId = null;
	private List<Floor> fdcRecord = new ArrayList<Floor>();
	private List<Floor> masterRecord = new ArrayList<Floor>();
	private int srcIndex = 0;
	private int mstIndex = 0;
	private int fdcRecordsRead = 0;
	private int masterRecordsRead = 0;
	private int newFloorsInserted = 0;
	private int existingFloorsReactivated = 0;
	private int existingFloorsUpdated = 0;
	private int existingFloorsDeactivated = 0;
	private int failedInserts = 0;
	private int failedUpdates = 0;
	private int failedHistoryInserts = 0;
	private int matchesFound = 0;
	private int dataMatchesFound = 0;
	private int up2dateServiceCalls = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		floorUpdateBegin();
		while (srcIndex < fdcRecord.size() || mstIndex < masterRecord.size()) {
			if (srcIndex >= fdcRecord.size()) {
				// no more fdc records -- everything else is a delete
				if (log.isDebugEnabled()) {
					log.debug("fdcRecord is null; masterRecord.getId(): " + masterRecord.get(mstIndex).getId());
				}
				processDelete();
			} else if (mstIndex >= masterRecord.size()) {
				// no more master records -- everything else is an add
				if (log.isDebugEnabled()) {
					log.debug("masterRecord is null; fdcRecord.getId(): " + fdcRecord.get(srcIndex).getId());
				}
				processAdd();
			} else if (fdcRecord.get(srcIndex).getId().equals(masterRecord.get(mstIndex).getId())) {
				// no change from master -- this is a match
				if (log.isDebugEnabled()) {
					log.debug("masterRecord == fdcRecord; id: " + fdcRecord.get(srcIndex).getId());
				}
				processMatch();
			} else if (fdcRecord.get(srcIndex).getId().compareTo(masterRecord.get(mstIndex).getId()) < 0) {
				// fdc < master -- this is an add
				if (log.isDebugEnabled()) {
					log.debug("fdcRecord < masterRecord; fdcRecord.getId(): " + fdcRecord.get(srcIndex).getId() + "; masterRecord.getId(): " + masterRecord.get(mstIndex).getId());
				}
				processAdd();
			} else {
				// master < fdc -- this is a delete
				if (log.isDebugEnabled()) {
					log.debug("fdcRecord > masterRecord; fdcRecord.getId(): " + fdcRecord.get(srcIndex).getId() + "; masterRecord.getId(): " + masterRecord.get(mstIndex).getId());
				}
				processDelete();
			}
		}
		return floorUpdateEnd();
	}

	private void floorUpdateBegin() throws Exception {
		log.info("MasterFloorUpdate starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify mstDataSource
		if (mstDataSource == null) {
			throw new IllegalArgumentException("Required property \"mstDataSource\" missing or invalid.");
		} else {
			try {
				mstConn = mstDataSource.getConnection();
				log.info("Connection established to mstDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to mstDataSource: " + e, e);
			}
		}
		// verify srcDataSource
		if (srcDataSource == null) {
			throw new IllegalArgumentException("Required property \"srcDataSource\" missing or invalid.");
		} else {
			try {
				srcConn = srcDataSource.getConnection();
				log.info("Connection established to srcDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to srcDataSource: " + e, e);
			}
		}
		// verify up2dateService
		if (up2dateService == null) {
			throw new IllegalArgumentException("Required property \"up2dateService\" missing or invalid.");
		}
		log.info("Up2Date service validated.");
		// verify publisherId
		if (StringUtils.isEmpty(publisherId)) {
			throw new IllegalArgumentException("Required property \"publisherId\" missing or invalid.");
		}
		log.info("Using publisherId " + publisherId);

		// set up local host address
		if (log.isDebugEnabled()) {
			log.debug("Setting up local host address");
		}
		InetAddress localMachine = InetAddress.getLocalHost();
		if (localMachine != null) {
			localAddress = localMachine.getHostAddress();
		}
		if (log.isDebugEnabled()) {
			log.debug("Local host address is " + localAddress);
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// connect to FD&C database
		srcConn = srcDataSource.getConnection();
		srcStmt = srcConn.createStatement();
		srcRs = srcStmt.executeQuery(SRC_SQL);
		while (srcRs.next()) {
			fdcRecordsRead++;
			fdcRecord.add(new Floor(srcRs.getString("ID"), srcRs.getInt("LEVEL"), srcRs.getString("PARENT"), srcRs.getString("NAME"), srcRs.getString("FULL_NAME"), srcRs.getString("ADDRESS"), srcRs.getString("CITY"), srcRs.getString("STATE"), srcRs.getString("ZIP"), srcRs.getString("COUNTRY"), srcRs.getString("LONGITUDE"), srcRs.getString("LATITUDE"), srcRs.getString("ALTITUDE"), srcRs.getString("BUILDING"), srcRs.getString("FLOOR"), srcRs.getString("ROOM"), srcRs.getString("GOOGLE_MAPS_URL"), srcRs.getString("DEPARTMENT")));
		}
		srcRs.close();
		srcStmt.close();
		srcConn.close();

		// connect to MasterData database
		mstConn = mstDataSource.getConnection();
		mstStmt = mstConn.createStatement();
		mstRs = mstStmt.executeQuery(MST_SQL);
		while (mstRs.next()) {
			masterRecordsRead++;
			masterRecord.add(new Floor(mstRs.getString("ID"), mstRs.getInt("LEVEL"), mstRs.getString("PARENT"), mstRs.getString("NAME"), mstRs.getString("FULL_NAME"), mstRs.getString("ADDRESS"), mstRs.getString("CITY"), mstRs.getString("STATE"), mstRs.getString("ZIP"), mstRs.getString("COUNTRY"), mstRs.getString("LONGITUDE"), mstRs.getString("LATITUDE"), mstRs.getString("ALTITUDE"), mstRs.getString("BUILDING"), mstRs.getString("FLOOR"), mstRs.getString("ROOM"), mstRs.getString("GOOGLE_MAPS_URL"), mstRs.getString("DEPARTMENT")));
		}
		mstRs.close();
		mstStmt.close();
	}

    private List<BatchJobServiceStatistic> floorUpdateEnd() throws Exception {
		// close MasterData connection
		mstConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (fdcRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("FD&C Floor records read", BatchJobService.FORMAT_INTEGER, new BigInteger(fdcRecordsRead + "")));
		}
		if (masterRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Master Floor records read", BatchJobService.FORMAT_INTEGER, new BigInteger(masterRecordsRead + "")));
		}
		if (matchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Floors found in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(matchesFound + "")));
		}
		if (dataMatchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Floors the same in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(dataMatchesFound + "")));
		}
		if (newFloorsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("New floors added", BatchJobService.FORMAT_INTEGER, new BigInteger(newFloorsInserted + "")));
		}
		if (existingFloorsReactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing floors reactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingFloorsReactivated + "")));
		}
		if (existingFloorsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing floors updated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingFloorsUpdated + "")));
		}
		if (existingFloorsDeactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing floors deactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingFloorsDeactivated + "")));
		}
		if (failedInserts > 0) {
			stats.add(new BatchJobServiceStatistic("Failed insert attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedInserts + "")));
		}
		if (failedUpdates > 0) {
			stats.add(new BatchJobServiceStatistic("Failed update attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedUpdates + "")));
		}
		if (failedHistoryInserts > 0) {
			stats.add(new BatchJobServiceStatistic("Failed history insert attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedHistoryInserts + "")));
		}
		if (up2dateServiceCalls > 0) {
			stats.add(new BatchJobServiceStatistic("Calls made to the Up2Date service", BatchJobService.FORMAT_INTEGER, new BigInteger(String.valueOf(up2dateServiceCalls))));
		}

		// end job
		log.info("MasterFloorUpdate complete.");

		return stats;
	}

	/**
	 * <p>Processes a match.</p>
	 */
	private void processMatch() throws Exception {
		matchesFound++;
		if (masterRecord.get(mstIndex).equals(fdcRecord.get(srcIndex))) {
			dataMatchesFound++;
		} else {
			updateFloor(masterRecord.get(mstIndex), fdcRecord.get(srcIndex));
		}
		srcIndex++;
		mstIndex++;
	}

	/**
	 * <p>Processes an add.</p>
	 */
	private void processAdd() throws Exception {
		insertFloor(fdcRecord.get(srcIndex));
		srcIndex++;
	}

	/**
	 * <p>Processes a delete.</p>
	 */
	private void processDelete() throws Exception {
		deleteFloor(masterRecord.get(mstIndex));
		mstIndex++;
	}

	private void insertFloor(Floor newFloor) {
		if (log.isDebugEnabled()) {
			log.debug("Inserting new Floor: " + newFloor.getId());
			log.debug("Using the following SQL: " + INSERT_SQL);
		}

		// insert into database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(INSERT_SQL);
			ps.setString(1, newFloor.getId());
			ps.setString(2, newFloor.getParent());
			ps.setString(3, newFloor.getName());
			ps.setString(4, newFloor.getFullName());
			ps.setString(5, newFloor.getAddress());
			ps.setString(6, newFloor.getCity());
			ps.setString(7, newFloor.getState());
			ps.setString(8, newFloor.getZip());
			ps.setString(9, newFloor.getCountry());
			ps.setString(10, newFloor.getLongitude());
			ps.setString(11, newFloor.getLatitude());
			ps.setString(12, newFloor.getAltitude());
			ps.setString(13, newFloor.getBuilding());
			ps.setString(14, newFloor.getFloor());
			ps.setString(15, newFloor.getMapsURL());
			ps.setString(16, USER_ID);
			ps.setString(17, localAddress);
			ps.setString(18, USER_ID);
			ps.setString(19, localAddress);
			if (ps.executeUpdate() > 0) {
				newFloorsInserted++;
				// publish
				Properties properties = new Properties();
				setProperty(properties, "id", newFloor.getId());
				setProperty(properties, "level", "2");
				setProperty(properties, "parent", newFloor.getParent());
				setProperty(properties, "name", newFloor.getName());
				setProperty(properties, "fullName", newFloor.getFullName());
				setProperty(properties, "address", newFloor.getAddress());
				setProperty(properties, "city", newFloor.getCity());
				setProperty(properties, "state", newFloor.getState());
				setProperty(properties, "zip", newFloor.getZip());
				setProperty(properties, "country", newFloor.getCountry());
				setProperty(properties, "longitude", newFloor.getLongitude());
				setProperty(properties, "latitude", newFloor.getLatitude());
				setProperty(properties, "altitude", newFloor.getAltitude());
				setProperty(properties, "building", newFloor.getBuilding());
				setProperty(properties, "floor", newFloor.getFloor());
				setProperty(properties, "googleMapsURL", newFloor.getMapsURL());
				up2dateService.post(new Update(publisherId, "add", properties));
				up2dateServiceCalls++;
			} else {
				failedInserts++;
				log.error("Unable to insert record for floor " + newFloor.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to insert record for floor " + newFloor.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void updateFloor(Floor oldFloor,  Floor newFloor) {
		if (log.isDebugEnabled()) {
			log.debug("Updating existing Floor: " + newFloor.getId());
			log.debug("Using the following SQL: " + UPDATE_SQL);
		}

		// update database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(UPDATE_SQL);
			ps.setString(1, newFloor.getParent());
			ps.setString(2, newFloor.getName());
			ps.setString(3, newFloor.getFullName());
			ps.setString(4, newFloor.getAddress());
			ps.setString(5, newFloor.getCity());
			ps.setString(6, newFloor.getState());
			ps.setString(7, newFloor.getZip());
			ps.setString(8, newFloor.getCountry());
			ps.setString(9, newFloor.getLongitude());
			ps.setString(10, newFloor.getLatitude());
			ps.setString(11, newFloor.getAltitude());
			ps.setString(12, newFloor.getBuilding());
			ps.setString(13, newFloor.getFloor());
			ps.setString(14, newFloor.getMapsURL());
			ps.setString(15, USER_ID);
			ps.setString(16, localAddress);
			ps.setString(17, newFloor.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", newFloor.getId());
				setProperty(properties, "level", "2");
				setProperty(properties, "parent", newFloor.getParent());
				setProperty(properties, "name", newFloor.getName());
				setProperty(properties, "fullName", newFloor.getFullName());
				setProperty(properties, "address", newFloor.getAddress());
				setProperty(properties, "city", newFloor.getCity());
				setProperty(properties, "state", newFloor.getState());
				setProperty(properties, "zip", newFloor.getZip());
				setProperty(properties, "country", newFloor.getCountry());
				setProperty(properties, "longitude", newFloor.getLongitude());
				setProperty(properties, "latitude", newFloor.getLatitude());
				setProperty(properties, "altitude", newFloor.getAltitude());
				setProperty(properties, "building", newFloor.getBuilding());
				setProperty(properties, "floor", newFloor.getFloor());
				setProperty(properties, "googleMapsURL", newFloor.getMapsURL());
				up2dateService.post(new Update(publisherId, "change", properties));
				up2dateServiceCalls++;
				updateFloorHistory(oldFloor, newFloor);
			} else {
				failedInserts++;
				log.error("Unable to update record for floor " + newFloor.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to update record for floor " + newFloor.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void deleteFloor(Floor oldFloor) {
		if (log.isDebugEnabled()) {
			log.debug("Deleting existing Floor: " + oldFloor.getId());
			log.debug("Using the following SQL: " + DELETE_SQL);
		}

		// delete from database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(DELETE_SQL);
			ps.setString(1, oldFloor.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", oldFloor.getId());
				setProperty(properties, "level", "2");
				setProperty(properties, "parent", oldFloor.getParent());
				setProperty(properties, "name", oldFloor.getName());
				setProperty(properties, "fullName", oldFloor.getFullName());
				setProperty(properties, "address", oldFloor.getAddress());
				setProperty(properties, "city", oldFloor.getCity());
				setProperty(properties, "state", oldFloor.getState());
				setProperty(properties, "zip", oldFloor.getZip());
				setProperty(properties, "country", oldFloor.getCountry());
				setProperty(properties, "longitude", oldFloor.getLongitude());
				setProperty(properties, "latitude", oldFloor.getLatitude());
				setProperty(properties, "altitude", oldFloor.getAltitude());
				setProperty(properties, "building", oldFloor.getBuilding());
				setProperty(properties, "floor", oldFloor.getFloor());
				setProperty(properties, "googleMapsURL", oldFloor.getMapsURL());
				up2dateService.post(new Update(publisherId, "delete", properties));
				up2dateServiceCalls++;
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + UPDATE_HISTORY_SQL);
				}
				ps.close();
				ps = mstConn.prepareStatement(UPDATE_HISTORY_SQL);
				ps.setString(1, oldFloor.getId());
				ps.setString(2, "RECORD");
				ps.setString(3, "EXISTED");
				ps.setString(4, "DELETED");
				ps.setString(5, USER_ID);
				ps.setString(6, localAddress);
				if (ps.executeUpdate() > 0) {
					if (log.isDebugEnabled()) {
						log.debug("Floor history log updated: Floor " + oldFloor.getId() + " deleted.");
					}
				}
			} else {
				failedInserts++;
				log.error("Unable to delete record for floor " + oldFloor.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to delete record for floor " + oldFloor.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	/**
	 * <p>Updates the floor history for any column updated.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newFloor the new values for the floor
	 * @param oldFloor the old values for the floor
	 */
	private void updateFloorHistory(Floor oldFloor,  Floor newFloor) {
		String id = newFloor.getId();
		if (log.isDebugEnabled()) {
			log.debug("Updating floor history for floor " + id);
		}

		// find changed columns
		List<Map<String,String>> change = newFloor.getChanges(oldFloor);

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for floor " + id);
			}
			PreparedStatement ps = null;
			try {
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + UPDATE_HISTORY_SQL);
				}
				ps = mstConn.prepareStatement(UPDATE_HISTORY_SQL);
				Iterator<Map<String,String>> i = change.iterator();
				while (i.hasNext()) {
					Map<String,String> thisChange = i.next();
					ps.setString(1, id);
					ps.setString(2, thisChange.get("COLUMN_NAME"));
					ps.setString(3, thisChange.get("OLD_VALUE"));
					ps.setString(4, thisChange.get("NEW_VALUE"));
					ps.setString(5, USER_ID);
					ps.setString(6, localAddress);
					if (ps.executeUpdate() > 0) {
						if (log.isDebugEnabled()) {
							log.debug("Floor history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update floor history for floor " + id + "; " + e.getMessage(), e);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception e) {
						//
					}
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No changes detected for floor " + id);
			}
		}
	}

	private class Floor {
		private String id = null;
		private int level = 0;
		private String parent = null;
		private String name = null;
		private String fullName = null;
		private String address = null;
		private String city = null;
		private String state = null;
		private String zip = null;
		private String country = null;
		private String longitude = null;
		private String latitude = null;
		private String altitude = null;
		private String building = null;
		private String floor = null;
		private String room = null;
		private String mapsURL = null;
		private String department = null;

		public Floor(String id, int level, String parent, String name, String fullName, String address, String city, String state, String zip, String country, String longitude, String latitude, String altitude, String building, String floor, String room, String mapsURL, String department) {
			this.id = id;
			this.level = level;
			this.parent = parent;
			this.name = name;
			this.fullName = fullName;
			this.address = address;
			this.city = city;
			this.state = state;
			this.zip = zip;
			this.country = country;
			this.longitude = longitude;
			this.latitude = latitude;
			this.altitude = altitude;
			this.building = building;
			this.floor = floor;
			this.room = room;
			this.mapsURL = mapsURL;
			this.department = department;
		}

		/**
		 * @return true if this Floor has the same values as the passed floor
		 */
		public boolean equals(Floor floor) {
			return isEqual(floor.id, this.id) &&
				floor.level == this.level &&
				isEqual(floor.parent, this.parent) &&
				isEqual(floor.name, this.name) &&
				isEqual(floor.fullName, this.fullName) &&
				isEqual(floor.address, this.address) &&
				isEqual(floor.city, this.city) &&
				isEqual(floor.state, this.state) &&
				isEqual(floor.zip, this.zip) &&
				isEqual(floor.country, this.country) &&
				isEqual(floor.longitude, this.longitude) &&
				isEqual(floor.latitude, this.latitude) &&
				isEqual(floor.altitude, this.altitude) &&
				isEqual(floor.floor, this.floor) &&
				isEqual(floor.floor, this.floor) &&
				isEqual(floor.room, this.room) &&
				isEqual(floor.mapsURL, this.mapsURL) &&
				isEqual(floor.department, this.department);
		}

		/**
		 * @return a list of changes between this floor and the floor passed
		 */
		public List<Map<String,String>> getChanges(Floor floor) {
			List<Map<String,String>> change = new ArrayList<Map<String,String>>();

			if (floor != null) {
				if (!isEqual(this.parent, floor.parent)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "PARENT");
					thisChange.put("OLD_VALUE", floor.parent);
					thisChange.put("NEW_VALUE", this.parent);
					change.add(thisChange);
				}
				if (!isEqual(this.name, floor.name)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "NAME");
					thisChange.put("OLD_VALUE", floor.name);
					thisChange.put("NEW_VALUE", this.name);
					change.add(thisChange);
				}
				if (!isEqual(this.fullName, floor.fullName)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FULL_NAME");
					thisChange.put("OLD_VALUE", floor.fullName);
					thisChange.put("NEW_VALUE", this.fullName);
					change.add(thisChange);
				}
				if (!isEqual(this.address, floor.address)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ADDRESS");
					thisChange.put("OLD_VALUE", floor.address);
					thisChange.put("NEW_VALUE", this.address);
					change.add(thisChange);
				}
				if (!isEqual(this.city, floor.city)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "CITY");
					thisChange.put("OLD_VALUE", floor.city);
					thisChange.put("NEW_VALUE", this.city);
					change.add(thisChange);
				}
				if (!isEqual(this.state, floor.state)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "STATE");
					thisChange.put("OLD_VALUE", floor.state);
					thisChange.put("NEW_VALUE", this.state);
					change.add(thisChange);
				}
				if (!isEqual(this.zip, floor.zip)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ZIP");
					thisChange.put("OLD_VALUE", floor.zip);
					thisChange.put("NEW_VALUE", this.zip);
					change.add(thisChange);
				}
				if (!isEqual(this.country, floor.country)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "COUNTRY");
					thisChange.put("OLD_VALUE", floor.country);
					thisChange.put("NEW_VALUE", this.country);
					change.add(thisChange);
				}
				if (!isEqual(this.longitude, floor.longitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LONGITUDE");
					thisChange.put("OLD_VALUE", floor.longitude);
					thisChange.put("NEW_VALUE", this.longitude);
					change.add(thisChange);
				}
				if (!isEqual(this.latitude, floor.latitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LATITUDE");
					thisChange.put("OLD_VALUE", floor.latitude);
					thisChange.put("NEW_VALUE", this.latitude);
					change.add(thisChange);
				}
				if (!isEqual(this.altitude, floor.altitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ALTITUDE");
					thisChange.put("OLD_VALUE", floor.altitude);
					thisChange.put("NEW_VALUE", this.altitude);
					change.add(thisChange);
				}
				if (!isEqual(this.building, floor.building)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "BUILDING");
					thisChange.put("OLD_VALUE", floor.building);
					thisChange.put("NEW_VALUE", this.building);
					change.add(thisChange);
				}
				if (!isEqual(this.floor, floor.floor)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FLOOR");
					thisChange.put("OLD_VALUE", floor.floor);
					thisChange.put("NEW_VALUE", this.floor);
					change.add(thisChange);
				}
				if (!isEqual(this.room, floor.room)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ROOM");
					thisChange.put("OLD_VALUE", floor.room);
					thisChange.put("NEW_VALUE", this.room);
					change.add(thisChange);
				}
				if (!isEqual(this.mapsURL, floor.mapsURL)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "GOOGLE_MAPS_URL");
					thisChange.put("OLD_VALUE", floor.mapsURL);
					thisChange.put("NEW_VALUE", this.mapsURL);
					change.add(thisChange);
				}
				if (!isEqual(this.department, floor.department)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "DEPARTMENT");
					thisChange.put("OLD_VALUE", floor.department);
					thisChange.put("NEW_VALUE", this.department);
					change.add(thisChange);
				}
			}

			return change;
		}

		public String getId() {
			return id;
		}

		public String getParent() {
			return parent;
		}

		public String getName() {
			return name;
		}

		public String getFullName() {
			return fullName;
		}

		public String getAddress() {
			return address;
		}

		public String getCity() {
			return city;
		}

		public String getState() {
			return state;
		}

		public String getZip() {
			return zip;
		}

		public String getCountry() {
			return country;
		}

		public String getLongitude() {
			return longitude;
		}

		public String getLatitude() {
			return latitude;
		}

		public String getAltitude() {
			return altitude;
		}

		public String getBuilding() {
			return building;
		}

		public String getFloor() {
			return floor;
		}

		public String getMapsURL() {
			return mapsURL;
		}
	}

	private static void setProperty(Properties properties, String key, String value) {
		if (StringUtils.isNotEmpty(key) && StringUtils.isEmpty(value)) {
			properties.setProperty(key.trim(), value.trim());
		}
	}

	private static boolean isEqual(String string1, String string2) {
		boolean isEqual = false;

		if (StringUtils.isEmpty(string1) && StringUtils.isEmpty(string2)) {
			isEqual = true;
		} else if (StringUtils.isNotEmpty(string1) && StringUtils.isNotEmpty(string2)) {
			isEqual = string1.trim().equals(string2.trim());
		}

		return isEqual;
	}

	/**
	 * @param mstDataSource the mstDataSource to set
	 */
	public void setMstDataSource(DataSource mstDataSource) {
		this.mstDataSource = mstDataSource;
	}

	/**
	 * @param srcDataSource the srcDataSource to set
	 */
	public void setSrcDataSource(DataSource srcDataSource) {
		this.srcDataSource = srcDataSource;
	}

	/**
	 * @param up2dateService the up2dateService to set
	 */
	public void setUp2dateService(Up2DateService up2dateService) {
		this.up2dateService = up2dateService;
	}

	/**
	 * @param publisherId the publisherId to set
	 */
	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}
}
