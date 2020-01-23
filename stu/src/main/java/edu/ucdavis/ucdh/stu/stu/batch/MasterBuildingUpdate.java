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
 * <p>Updates the Master building list from the FD&C building list.</p>
 */
public class MasterBuildingUpdate implements SpringBatchJob {
	private static final String SRC_SQL = "SELECT UCDHS_Bldg_Num AS ID, 1 AS [LEVEL], NULL AS PARENT, Official_Name AS NAME, Official_Name AS FULL_NAME, Address AS ADDRESS, City AS CITY, State AS STATE, Zip AS ZIP, Country_Code AS COUNTRY, Longitude AS LONGITUDE, Latitude AS LATITUDE, Official_Name AS BUILDING, NULL AS FLOOR, NULL AS ROOM, Google_Maps_URL AS GOOGLE_MAPS_URL, NULL AS DEPARTMENT FROM Building WHERE UCDHS_Bldg_Num > '' ORDER BY ID";
	private static final String MST_SQL = "SELECT ID, [LEVEL], PARENT, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, BUILDING, FLOOR, ROOM, GOOGLE_MAPS_URL, DEPARTMENT FROM LOCATION WHERE [LEVEL]=1 ORDER BY ID";
	private static final String INSERT_SQL = "INSERT INTO LOCATION (ID, LEVEL, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, BUILDING, GOOGLE_MAPS_URL, CREATED_BY, CREATED_ON, CREATED_IP, UPDATED_BY, UPDATED_ON, UPDATED_IP, UPDATE_COUNT) VALUES(?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, getdate(), ?, 0)";
	private static final String UPDATE_SQL = "UPDATE LOCATION SET NAME=?, FULL_NAME=?, ADDRESS=?, CITY=?, STATE=?, ZIP=?, COUNTRY=?, LONGITUDE=?, LATITUDE=?, BUILDING=?, GOOGLE_MAPS_URL=?, DEPARTMENT=?, UPDATED_BY=?, UPDATED_ON=getdate(), UPDATED_IP=?, UPDATE_COUNT=UPDATE_COUNT+1 WHERE ID=?";
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
	private List<Building> fdcRecord = new ArrayList<Building>();
	private List<Building> masterRecord = new ArrayList<Building>();
	private int srcIndex = 0;
	private int mstIndex = 0;
	private int fdcRecordsRead = 0;
	private int masterRecordsRead = 0;
	private int newBuildingsInserted = 0;
	private int existingBuildingsReactivated = 0;
	private int existingBuildingsUpdated = 0;
	private int existingBuildingsDeactivated = 0;
	private int failedInserts = 0;
	private int failedUpdates = 0;
	private int failedHistoryInserts = 0;
	private int matchesFound = 0;
	private int dataMatchesFound = 0;
	private int up2dateServiceCalls = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		buildingUpdateBegin();
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
		return buildingUpdateEnd();
	}

	private void buildingUpdateBegin() throws Exception {
		log.info("MasterBuildingUpdate starting ...");
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
			fdcRecord.add(new Building(srcRs.getString("ID"), srcRs.getInt("LEVEL"), srcRs.getString("PARENT"), srcRs.getString("NAME"), srcRs.getString("FULL_NAME"), srcRs.getString("ADDRESS"), srcRs.getString("CITY"), srcRs.getString("STATE"), srcRs.getString("ZIP"), srcRs.getString("COUNTRY"), srcRs.getString("LONGITUDE"), srcRs.getString("LATITUDE"), srcRs.getString("BUILDING"), srcRs.getString("FLOOR"), srcRs.getString("ROOM"), srcRs.getString("GOOGLE_MAPS_URL"), srcRs.getString("DEPARTMENT")));
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
			masterRecord.add(new Building(mstRs.getString("ID"), mstRs.getInt("LEVEL"), mstRs.getString("PARENT"), mstRs.getString("NAME"), mstRs.getString("FULL_NAME"), mstRs.getString("ADDRESS"), mstRs.getString("CITY"), mstRs.getString("STATE"), mstRs.getString("ZIP"), mstRs.getString("COUNTRY"), mstRs.getString("LONGITUDE"), mstRs.getString("LATITUDE"), mstRs.getString("BUILDING"), mstRs.getString("FLOOR"), mstRs.getString("ROOM"), mstRs.getString("GOOGLE_MAPS_URL"), mstRs.getString("DEPARTMENT")));
		}
		mstRs.close();
		mstStmt.close();
	}

    private List<BatchJobServiceStatistic> buildingUpdateEnd() throws Exception {
		// close MasterData connection
		mstConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (fdcRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("FD&C Building records read", BatchJobService.FORMAT_INTEGER, new BigInteger(fdcRecordsRead + "")));
		}
		if (masterRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Master Building records read", BatchJobService.FORMAT_INTEGER, new BigInteger(masterRecordsRead + "")));
		}
		if (matchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Buildings found in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(matchesFound + "")));
		}
		if (dataMatchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Buildings the same in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(dataMatchesFound + "")));
		}
		if (newBuildingsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("New buildings added", BatchJobService.FORMAT_INTEGER, new BigInteger(newBuildingsInserted + "")));
		}
		if (existingBuildingsReactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing buildings reactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingBuildingsReactivated + "")));
		}
		if (existingBuildingsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing buildings updated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingBuildingsUpdated + "")));
		}
		if (existingBuildingsDeactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing buildings deactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingBuildingsDeactivated + "")));
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
		log.info("MasterBuildingUpdate complete.");

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
			updateBuilding(masterRecord.get(mstIndex), fdcRecord.get(srcIndex));
		}
		srcIndex++;
		mstIndex++;
	}

	/**
	 * <p>Processes an add.</p>
	 */
	private void processAdd() throws Exception {
		insertBuilding(fdcRecord.get(srcIndex));
		srcIndex++;
	}

	/**
	 * <p>Processes a delete.</p>
	 */
	private void processDelete() throws Exception {
		deleteBuilding(masterRecord.get(mstIndex));
		mstIndex++;
	}

	private void insertBuilding(Building newBuilding) {
		if (log.isDebugEnabled()) {
			log.debug("Inserting new Building: " + newBuilding.getId());
			log.debug("Using the following SQL: " + INSERT_SQL);
		}

		// insert into database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(INSERT_SQL);
			ps.setString(1, newBuilding.getId());
			ps.setString(2, newBuilding.getName());
			ps.setString(3, newBuilding.getFullName());
			ps.setString(4, newBuilding.getAddress());
			ps.setString(5, newBuilding.getCity());
			ps.setString(6, newBuilding.getState());
			ps.setString(7, newBuilding.getZip());
			ps.setString(8, newBuilding.getCountry());
			ps.setString(9, newBuilding.getLongitude());
			ps.setString(10, newBuilding.getLatitude());
			ps.setString(11, newBuilding.getBuilding());
			ps.setString(12, newBuilding.getMapsURL());
			ps.setString(13, USER_ID);
			ps.setString(14, localAddress);
			ps.setString(15, USER_ID);
			ps.setString(16, localAddress);
			if (ps.executeUpdate() > 0) {
				newBuildingsInserted++;
				// publish
				Properties properties = new Properties();
				setProperty(properties, "id", newBuilding.getId());
				setProperty(properties, "level", "1");
				setProperty(properties, "name", newBuilding.getName());
				setProperty(properties, "fullName", newBuilding.getFullName());
				setProperty(properties, "address", newBuilding.getAddress());
				setProperty(properties, "city", newBuilding.getCity());
				setProperty(properties, "state", newBuilding.getState());
				setProperty(properties, "zip", newBuilding.getZip());
				setProperty(properties, "country", newBuilding.getCountry());
				setProperty(properties, "longitude", newBuilding.getLongitude());
				setProperty(properties, "latitude", newBuilding.getLatitude());
				setProperty(properties, "building", newBuilding.getBuilding());
				setProperty(properties, "googleMapsURL", newBuilding.getMapsURL());
				up2dateService.post(new Update(publisherId, "add", properties));
				up2dateServiceCalls++;
			} else {
				failedInserts++;
				log.error("Unable to insert record for building " + newBuilding.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to insert record for building " + newBuilding.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void updateBuilding(Building oldBuilding,  Building newBuilding) {
		if (log.isDebugEnabled()) {
			log.debug("Updating existing Building: " + newBuilding.getId());
			log.debug("Using the following SQL: " + UPDATE_SQL);
		}

		// update database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(UPDATE_SQL);
			ps.setString(1, newBuilding.getName());
			ps.setString(2, newBuilding.getFullName());
			ps.setString(3, newBuilding.getAddress());
			ps.setString(4, newBuilding.getCity());
			ps.setString(5, newBuilding.getState());
			ps.setString(6, newBuilding.getZip());
			ps.setString(7, newBuilding.getCountry());
			ps.setString(8, newBuilding.getLongitude());
			ps.setString(9, newBuilding.getLatitude());
			ps.setString(10, newBuilding.getBuilding());
			ps.setString(11, newBuilding.getMapsURL());
			ps.setString(12, USER_ID);
			ps.setString(13, localAddress);
			ps.setString(14, newBuilding.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", newBuilding.getId());
				setProperty(properties, "level", "1");
				setProperty(properties, "name", newBuilding.getName());
				setProperty(properties, "fullName", newBuilding.getFullName());
				setProperty(properties, "address", newBuilding.getAddress());
				setProperty(properties, "city", newBuilding.getCity());
				setProperty(properties, "state", newBuilding.getState());
				setProperty(properties, "zip", newBuilding.getZip());
				setProperty(properties, "country", newBuilding.getCountry());
				setProperty(properties, "longitude", newBuilding.getLongitude());
				setProperty(properties, "latitude", newBuilding.getLatitude());
				setProperty(properties, "building", newBuilding.getBuilding());
				setProperty(properties, "googleMapsURL", newBuilding.getMapsURL());
				up2dateService.post(new Update(publisherId, "change", properties));
				up2dateServiceCalls++;
				updateBuildingHistory(oldBuilding, newBuilding);
			} else {
				failedInserts++;
				log.error("Unable to update record for building " + newBuilding.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to update record for building " + newBuilding.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void deleteBuilding(Building oldBuilding) {
		if (log.isDebugEnabled()) {
			log.debug("Deleting existing Building: " + oldBuilding.getId());
			log.debug("Using the following SQL: " + DELETE_SQL);
		}

		// delete from database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(DELETE_SQL);
			ps.setString(1, oldBuilding.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", oldBuilding.getId());
				setProperty(properties, "level", "1");
				setProperty(properties, "name", oldBuilding.getName());
				setProperty(properties, "fullName", oldBuilding.getFullName());
				setProperty(properties, "address", oldBuilding.getAddress());
				setProperty(properties, "city", oldBuilding.getCity());
				setProperty(properties, "state", oldBuilding.getState());
				setProperty(properties, "zip", oldBuilding.getZip());
				setProperty(properties, "country", oldBuilding.getCountry());
				setProperty(properties, "longitude", oldBuilding.getLongitude());
				setProperty(properties, "latitude", oldBuilding.getLatitude());
				setProperty(properties, "building", oldBuilding.getBuilding());
				setProperty(properties, "googleMapsURL", oldBuilding.getMapsURL());
				up2dateService.post(new Update(publisherId, "delete", properties));
				up2dateServiceCalls++;
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + UPDATE_HISTORY_SQL);
				}
				ps.close();
				ps = mstConn.prepareStatement(UPDATE_HISTORY_SQL);
				ps.setString(1, oldBuilding.getId());
				ps.setString(2, "RECORD");
				ps.setString(3, "EXISTED");
				ps.setString(4, "DELETED");
				ps.setString(5, USER_ID);
				ps.setString(6, localAddress);
				if (ps.executeUpdate() > 0) {
					if (log.isDebugEnabled()) {
						log.debug("Building history log updated: Building " + oldBuilding.getId() + " deleted.");
					}
				}
			} else {
				failedInserts++;
				log.error("Unable to delete record for building " + oldBuilding.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to delete record for building " + oldBuilding.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	/**
	 * <p>Updates the building history for any column updated.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newBuilding the new values for the building
	 * @param oldBuilding the old values for the building
	 */
	private void updateBuildingHistory(Building oldBuilding,  Building newBuilding) {
		String id = newBuilding.getId();
		if (log.isDebugEnabled()) {
			log.debug("Updating building history for building " + id);
		}

		// find changed columns
		List<Map<String,String>> change = newBuilding.getChanges(oldBuilding);

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for building " + id);
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
							log.debug("Building history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update building history for building " + id + "; " + e.getMessage(), e);
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
				log.debug("No changes detected for building " + id);
			}
		}
	}

	private class Building {
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
		private String building = null;
		private String floor = null;
		private String room = null;
		private String mapsURL = null;
		private String department = null;

		public Building(String id, int level, String parent, String name, String fullName, String address, String city, String state, String zip, String country, String longitude, String latitude, String building, String floor, String room, String mapsURL, String department) {
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
			this.building = building;
			this.floor = floor;
			this.room = room;
			this.mapsURL = mapsURL;
			this.department = department;
		}

		/**
		 * @return true if this Building has the same values as the passed building
		 */
		public boolean equals(Building building) {
			return isEqual(building.id, this.id) &&
				building.level == this.level &&
				isEqual(building.parent, this.parent) &&
				isEqual(building.name, this.name) &&
				isEqual(building.fullName, this.fullName) &&
				isEqual(building.address, this.address) &&
				isEqual(building.city, this.city) &&
				isEqual(building.state, this.state) &&
				isEqual(building.zip, this.zip) &&
				isEqual(building.country, this.country) &&
				isEqual(building.longitude, this.longitude) &&
				isEqual(building.latitude, this.latitude) &&
				isEqual(building.building, this.building) &&
				isEqual(building.floor, this.floor) &&
				isEqual(building.room, this.room) &&
				isEqual(building.mapsURL, this.mapsURL) &&
				isEqual(building.department, this.department);
		}

		/**
		 * @return a list of changes between this building and the building passed
		 */
		public List<Map<String,String>> getChanges(Building building) {
			List<Map<String,String>> change = new ArrayList<Map<String,String>>();

			if (building != null) {
				if (!isEqual(this.name, building.name)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "NAME");
					thisChange.put("OLD_VALUE", building.name);
					thisChange.put("NEW_VALUE", this.name);
					change.add(thisChange);
				}
				if (!isEqual(this.fullName, building.fullName)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FULL_NAME");
					thisChange.put("OLD_VALUE", building.fullName);
					thisChange.put("NEW_VALUE", this.fullName);
					change.add(thisChange);
				}
				if (!isEqual(this.address, building.address)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ADDRESS");
					thisChange.put("OLD_VALUE", building.address);
					thisChange.put("NEW_VALUE", this.address);
					change.add(thisChange);
				}
				if (!isEqual(this.city, building.city)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "CITY");
					thisChange.put("OLD_VALUE", building.city);
					thisChange.put("NEW_VALUE", this.city);
					change.add(thisChange);
				}
				if (!isEqual(this.state, building.state)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "STATE");
					thisChange.put("OLD_VALUE", building.state);
					thisChange.put("NEW_VALUE", this.state);
					change.add(thisChange);
				}
				if (!isEqual(this.zip, building.zip)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ZIP");
					thisChange.put("OLD_VALUE", building.zip);
					thisChange.put("NEW_VALUE", this.zip);
					change.add(thisChange);
				}
				if (!isEqual(this.country, building.country)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "COUNTRY");
					thisChange.put("OLD_VALUE", building.country);
					thisChange.put("NEW_VALUE", this.country);
					change.add(thisChange);
				}
				if (!isEqual(this.longitude, building.longitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LONGITUDE");
					thisChange.put("OLD_VALUE", building.longitude);
					thisChange.put("NEW_VALUE", this.longitude);
					change.add(thisChange);
				}
				if (!isEqual(this.latitude, building.latitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LATITUDE");
					thisChange.put("OLD_VALUE", building.latitude);
					thisChange.put("NEW_VALUE", this.latitude);
					change.add(thisChange);
				}
				if (!isEqual(this.building, building.building)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "BUILDING");
					thisChange.put("OLD_VALUE", building.building);
					thisChange.put("NEW_VALUE", this.building);
					change.add(thisChange);
				}
				if (!isEqual(this.floor, building.floor)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FLOOR");
					thisChange.put("OLD_VALUE", building.floor);
					thisChange.put("NEW_VALUE", this.floor);
					change.add(thisChange);
				}
				if (!isEqual(this.room, building.room)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ROOM");
					thisChange.put("OLD_VALUE", building.room);
					thisChange.put("NEW_VALUE", this.room);
					change.add(thisChange);
				}
				if (!isEqual(this.mapsURL, building.mapsURL)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "GOOGLE_MAPS_URL");
					thisChange.put("OLD_VALUE", building.mapsURL);
					thisChange.put("NEW_VALUE", this.mapsURL);
					change.add(thisChange);
				}
				if (!isEqual(this.department, building.department)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "DEPARTMENT");
					thisChange.put("OLD_VALUE", building.department);
					thisChange.put("NEW_VALUE", this.department);
					change.add(thisChange);
				}
			}

			return change;
		}

		public String getId() {
			return id;
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

		public String getBuilding() {
			return building;
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
