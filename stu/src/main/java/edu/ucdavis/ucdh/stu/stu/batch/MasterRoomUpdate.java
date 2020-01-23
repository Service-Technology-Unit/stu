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
 * <p>Updates the Master Room list from the FD&C room list.</p>
 */
public class MasterRoomUpdate implements SpringBatchJob {
	private static final String SRC_SQL = "SELECT b.UCDHS_Bldg_Num + '-' + f.Floor_Code + '-' + r.Room AS ID, 3 AS [LEVEL], b.UCDHS_Bldg_Num + '-' + f.Floor_Code AS PARENT, r.Room AS NAME, b.Official_Name + ', Rm ' + r.Room AS FULL_NAME, b.Address AS ADDRESS, b.City AS CITY, b.State AS STATE, b.Zip AS ZIP, b.Country_Code AS COUNTRY, r.Longitude AS LONGITUDE, r.Latitude AS LATITUDE, f.Above_Ground_Inches AS ALTITUDE, b.Official_Name AS BUILDING, f.Floor_Name AS FLOOR, r.Room + CASE WHEN r.Room_Name > '' THEN ' (' + r.Room_Name + ')' ELSE '' END AS ROOM, b.Google_Maps_URL AS GOOGLE_MAPS_URL, m.ID AS DEPARTMENT FROM Room r LEFT OUTER JOIN Floor f ON f.Floor_Key=r.Floor_Key LEFT OUTER JOIN Building b ON b.Bldg_Key=r.Bldg_Key LEFT OUTER JOIN DeptRoom dr ON dr.room_key=r.Room_Key AND dr.room_share_percent=100.0 LEFT OUTER JOIN Dept d ON d.space_acctng_dept_Key=dr.space_acctng_dept_key LEFT OUTER JOIN HSHPSQL05.MasterData.dbo.DEPARTMENT m ON ISNULL(m.COST_CENTER_ID, N'') > '' AND m.COST_CENTER_ID=SUBSTRING(d.cost_center, 11,4) WHERE f.Floor_Code<>'XX' AND ISNULL(b.UCDHS_Bldg_Num,'')<>'' ORDER BY ID";
	private static final String MST_SQL = "SELECT ID, [LEVEL], PARENT, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, ALTITUDE, BUILDING, FLOOR, ROOM, GOOGLE_MAPS_URL, DEPARTMENT FROM LOCATION WHERE [LEVEL]=3 ORDER BY CONVERT(varchar(MAX), ID)";
	private static final String INSERT_SQL = "INSERT INTO LOCATION (ID, LEVEL, PARENT, NAME, FULL_NAME, ADDRESS, CITY, STATE, ZIP, COUNTRY, LONGITUDE, LATITUDE, ALTITUDE, BUILDING, FLOOR, ROOM, GOOGLE_MAPS_URL, DEPARTMENT, CREATED_BY, CREATED_ON, CREATED_IP, UPDATED_BY, UPDATED_ON, UPDATED_IP, UPDATE_COUNT) VALUES(?, 3, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, getdate(), ?, 0)";
	private static final String UPDATE_SQL = "UPDATE LOCATION SET PARENT=?, NAME=?, FULL_NAME=?, ADDRESS=?, CITY=?, STATE=?, ZIP=?, COUNTRY=?, LONGITUDE=?, LATITUDE=?, ALTITUDE=?, BUILDING=?, FLOOR=?, ROOM=?, GOOGLE_MAPS_URL=?, DEPARTMENT=?, UPDATED_BY=?, UPDATED_ON=getdate(), UPDATED_IP=?, UPDATE_COUNT=UPDATE_COUNT+1 WHERE ID=?";
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
	private List<Room> fdcRecord = new ArrayList<Room>();
	private List<Room> masterRecord = new ArrayList<Room>();
	private int srcIndex = 0;
	private int mstIndex = 0;
	private int fdcRecordsRead = 0;
	private int masterRecordsRead = 0;
	private int newRoomsInserted = 0;
	private int existingRoomsReactivated = 0;
	private int existingRoomsUpdated = 0;
	private int existingRoomsDeactivated = 0;
	private int failedInserts = 0;
	private int failedUpdates = 0;
	private int failedHistoryInserts = 0;
	private int matchesFound = 0;
	private int dataMatchesFound = 0;
	private int up2dateServiceCalls = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		roomUpdateBegin();
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
		return roomUpdateEnd();
	}

	private void roomUpdateBegin() throws Exception {
		log.info("MasterRoomUpdate starting ...");
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
			fdcRecord.add(new Room(srcRs.getString("ID"), srcRs.getInt("LEVEL"), srcRs.getString("PARENT"), srcRs.getString("NAME"), srcRs.getString("FULL_NAME"), srcRs.getString("ADDRESS"), srcRs.getString("CITY"), srcRs.getString("STATE"), srcRs.getString("ZIP"), srcRs.getString("COUNTRY"), srcRs.getString("LONGITUDE"), srcRs.getString("LATITUDE"), srcRs.getString("ALTITUDE"), srcRs.getString("BUILDING"), srcRs.getString("FLOOR"), srcRs.getString("ROOM"), srcRs.getString("GOOGLE_MAPS_URL"), srcRs.getString("DEPARTMENT")));
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
			masterRecord.add(new Room(mstRs.getString("ID"), mstRs.getInt("LEVEL"), mstRs.getString("PARENT"), mstRs.getString("NAME"), mstRs.getString("FULL_NAME"), mstRs.getString("ADDRESS"), mstRs.getString("CITY"), mstRs.getString("STATE"), mstRs.getString("ZIP"), mstRs.getString("COUNTRY"), mstRs.getString("LONGITUDE"), mstRs.getString("LATITUDE"), mstRs.getString("ALTITUDE"), mstRs.getString("BUILDING"), mstRs.getString("FLOOR"), mstRs.getString("ROOM"), mstRs.getString("GOOGLE_MAPS_URL"), mstRs.getString("DEPARTMENT")));
		}
		mstRs.close();
		mstStmt.close();
	}

    private List<BatchJobServiceStatistic> roomUpdateEnd() throws Exception {
		// close MasterData connection
		mstConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (fdcRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("FD&C Room records read", BatchJobService.FORMAT_INTEGER, new BigInteger(fdcRecordsRead + "")));
		}
		if (masterRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Master Room records read", BatchJobService.FORMAT_INTEGER, new BigInteger(masterRecordsRead + "")));
		}
		if (matchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Rooms found in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(matchesFound + "")));
		}
		if (dataMatchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Rooms the same in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(dataMatchesFound + "")));
		}
		if (newRoomsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("New Rooms added", BatchJobService.FORMAT_INTEGER, new BigInteger(newRoomsInserted + "")));
		}
		if (existingRoomsReactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing Rooms reactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingRoomsReactivated + "")));
		}
		if (existingRoomsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing Rooms updated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingRoomsUpdated + "")));
		}
		if (existingRoomsDeactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing Rooms deactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingRoomsDeactivated + "")));
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
		log.info("MasterRoomUpdate complete.");

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
			updateRoom(masterRecord.get(mstIndex), fdcRecord.get(srcIndex));
		}
		srcIndex++;
		mstIndex++;
	}

	/**
	 * <p>Processes an add.</p>
	 */
	private void processAdd() throws Exception {
		insertRoom(fdcRecord.get(srcIndex));
		srcIndex++;
	}

	/**
	 * <p>Processes a delete.</p>
	 */
	private void processDelete() throws Exception {
		deleteRoom(masterRecord.get(mstIndex));
		mstIndex++;
	}

	private void insertRoom(Room newRoom) {
		if (log.isDebugEnabled()) {
			log.debug("Inserting new Room: " + newRoom.getId());
			log.debug("Using the following SQL: " + INSERT_SQL);
		}

		// insert into database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(INSERT_SQL);
			ps.setString(1, newRoom.getId());
			ps.setString(2, newRoom.getParent());
			ps.setString(3, newRoom.getName());
			ps.setString(4, newRoom.getFullName());
			ps.setString(5, newRoom.getAddress());
			ps.setString(6, newRoom.getCity());
			ps.setString(7, newRoom.getState());
			ps.setString(8, newRoom.getZip());
			ps.setString(9, newRoom.getCountry());
			ps.setString(10, newRoom.getLongitude());
			ps.setString(11, newRoom.getLatitude());
			ps.setString(12, newRoom.getAltitude());
			ps.setString(13, newRoom.getBuilding());
			ps.setString(14, newRoom.getFloor());
			ps.setString(15, newRoom.getRoom());
			ps.setString(16, newRoom.getMapsURL());
			ps.setString(17, newRoom.getDepartment());
			ps.setString(18, USER_ID);
			ps.setString(19, localAddress);
			ps.setString(20, USER_ID);
			ps.setString(21, localAddress);
			if (ps.executeUpdate() > 0) {
				newRoomsInserted++;
				// publish
				Properties properties = new Properties();
				setProperty(properties, "id", newRoom.getId());
				setProperty(properties, "level", "3");
				setProperty(properties, "parent", newRoom.getParent());
				setProperty(properties, "name", newRoom.getName());
				setProperty(properties, "fullName", newRoom.getFullName());
				setProperty(properties, "address", newRoom.getAddress());
				setProperty(properties, "city", newRoom.getCity());
				setProperty(properties, "state", newRoom.getState());
				setProperty(properties, "zip", newRoom.getZip());
				setProperty(properties, "country", newRoom.getCountry());
				setProperty(properties, "longitude", newRoom.getLongitude());
				setProperty(properties, "latitude", newRoom.getLatitude());
				setProperty(properties, "altitude", newRoom.getAltitude());
				setProperty(properties, "building", newRoom.getBuilding());
				setProperty(properties, "floor", newRoom.getFloor());
				setProperty(properties, "room", newRoom.getRoom());
				setProperty(properties, "googleMapsURL", newRoom.getMapsURL());
				setProperty(properties, "department", newRoom.getDepartment());
				up2dateService.post(new Update(publisherId, "add", properties));
				up2dateServiceCalls++;
			} else {
				failedInserts++;
				log.error("Unable to insert record for room " + newRoom.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to insert record for room " + newRoom.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void updateRoom(Room oldRoom,  Room newRoom) {
		if (log.isDebugEnabled()) {
			log.debug("Updating existing Room: " + newRoom.getId());
			log.debug("Using the following SQL: " + UPDATE_SQL);
		}

		// update database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(UPDATE_SQL);
			ps.setString(1, newRoom.getParent());
			ps.setString(2, newRoom.getName());
			ps.setString(3, newRoom.getFullName());
			ps.setString(4, newRoom.getAddress());
			ps.setString(5, newRoom.getCity());
			ps.setString(6, newRoom.getState());
			ps.setString(7, newRoom.getZip());
			ps.setString(8, newRoom.getCountry());
			ps.setString(9, newRoom.getLongitude());
			ps.setString(10, newRoom.getLatitude());
			ps.setString(11, newRoom.getAltitude());
			ps.setString(12, newRoom.getBuilding());
			ps.setString(13, newRoom.getFloor());
			ps.setString(14, newRoom.getRoom());
			ps.setString(15, newRoom.getMapsURL());
			ps.setString(16, newRoom.getDepartment());
			ps.setString(17, USER_ID);
			ps.setString(18, localAddress);
			ps.setString(19, newRoom.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", newRoom.getId());
				setProperty(properties, "level", "3");
				setProperty(properties, "parent", newRoom.getParent());
				setProperty(properties, "name", newRoom.getName());
				setProperty(properties, "fullName", newRoom.getFullName());
				setProperty(properties, "address", newRoom.getAddress());
				setProperty(properties, "city", newRoom.getCity());
				setProperty(properties, "state", newRoom.getState());
				setProperty(properties, "zip", newRoom.getZip());
				setProperty(properties, "country", newRoom.getCountry());
				setProperty(properties, "longitude", newRoom.getLongitude());
				setProperty(properties, "latitude", newRoom.getLatitude());
				setProperty(properties, "altitude", newRoom.getAltitude());
				setProperty(properties, "building", newRoom.getBuilding());
				setProperty(properties, "floor", newRoom.getFloor());
				setProperty(properties, "room", newRoom.getRoom());
				setProperty(properties, "googleMapsURL", newRoom.getMapsURL());
				setProperty(properties, "department", newRoom.getDepartment());
				up2dateService.post(new Update(publisherId, "change", properties));
				up2dateServiceCalls++;
				updateRoomHistory(oldRoom, newRoom);
			} else {
				failedInserts++;
				log.error("Unable to update record for room " + newRoom.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to update record for room " + newRoom.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void deleteRoom(Room oldRoom) {
		if (log.isDebugEnabled()) {
			log.debug("Deleting existing room: " + oldRoom.getId());
			log.debug("Using the following SQL: " + DELETE_SQL);
		}

		// delete from database
		PreparedStatement ps = null;
		try {
			ps = mstConn.prepareStatement(DELETE_SQL);
			ps.setString(1, oldRoom.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				setProperty(properties, "id", oldRoom.getId());
				setProperty(properties, "level", "3");
				setProperty(properties, "parent", oldRoom.getParent());
				setProperty(properties, "name", oldRoom.getName());
				setProperty(properties, "fullName", oldRoom.getFullName());
				setProperty(properties, "address", oldRoom.getAddress());
				setProperty(properties, "city", oldRoom.getCity());
				setProperty(properties, "state", oldRoom.getState());
				setProperty(properties, "zip", oldRoom.getZip());
				setProperty(properties, "country", oldRoom.getCountry());
				setProperty(properties, "longitude", oldRoom.getLongitude());
				setProperty(properties, "latitude", oldRoom.getLatitude());
				setProperty(properties, "altitude", oldRoom.getAltitude());
				setProperty(properties, "building", oldRoom.getBuilding());
				setProperty(properties, "floor", oldRoom.getFloor());
				setProperty(properties, "room", oldRoom.getRoom());
				setProperty(properties, "googleMapsURL", oldRoom.getMapsURL());
				setProperty(properties, "department", oldRoom.getDepartment());
				up2dateService.post(new Update(publisherId, "delete", properties));
				up2dateServiceCalls++;
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + UPDATE_HISTORY_SQL);
				}
				ps.close();
				ps = mstConn.prepareStatement(UPDATE_HISTORY_SQL);
				ps.setString(1, oldRoom.getId());
				ps.setString(2, "RECORD");
				ps.setString(3, "EXISTED");
				ps.setString(4, "DELETED");
				ps.setString(5, USER_ID);
				ps.setString(6, localAddress);
				if (ps.executeUpdate() > 0) {
					if (log.isDebugEnabled()) {
						log.debug("Room history log updated: Room " + oldRoom.getId() + " deleted.");
					}
				}
			} else {
				failedInserts++;
				log.error("Unable to delete record for room " + oldRoom.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to delete record for room " + oldRoom.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	/**
	 * <p>Updates the Room history for any column updated.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newRoom the new values for the room
	 * @param oldRoom the old values for the room
	 */
	private void updateRoomHistory(Room oldRoom,  Room newRoom) {
		String id = newRoom.getId();
		if (log.isDebugEnabled()) {
			log.debug("Updating room history for room " + id);
		}

		// find changed columns
		List<Map<String,String>> change = newRoom.getChanges(oldRoom);

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for room " + id);
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
							log.debug("Room history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update room history for room " + id + "; " + e.getMessage(), e);
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
				log.debug("No changes detected for room " + id);
			}
		}
	}

	private class Room {
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

		public Room(String id, int level, String parent, String name, String fullName, String address, String city, String state, String zip, String country, String longitude, String latitude, String altitude, String building, String floor, String room, String mapsURL, String department) {
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
		 * @return true if this room has the same values as the passed room
		 */
		public boolean equals(Room room) {
			return isEqual(room.id, this.id) &&
				room.level == this.level &&
				isEqual(room.parent, this.parent) &&
				isEqual(room.name, this.name) &&
				isEqual(room.fullName, this.fullName) &&
				isEqual(room.address, this.address) &&
				isEqual(room.city, this.city) &&
				isEqual(room.state, this.state) &&
				isEqual(room.zip, this.zip) &&
				isEqual(room.country, this.country) &&
				isEqual(room.longitude, this.longitude) &&
				isEqual(room.latitude, this.latitude) &&
				isEqual(room.altitude, this.altitude) &&
				isEqual(room.building, this.building) &&
				isEqual(room.floor, this.floor) &&
				isEqual(room.room, this.room) &&
				isEqual(room.mapsURL, this.mapsURL) &&
				isEqual(room.department, this.department);
		}

		/**
		 * @return a list of changes between this room and the room passed
		 */
		public List<Map<String,String>> getChanges(Room room) {
			List<Map<String,String>> change = new ArrayList<Map<String,String>>();

			if (room != null) {
				if (!isEqual(this.parent, room.parent)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "PARENT");
					thisChange.put("OLD_VALUE", room.parent);
					thisChange.put("NEW_VALUE", this.parent);
					change.add(thisChange);
				}
				if (!isEqual(this.name, room.name)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "NAME");
					thisChange.put("OLD_VALUE", room.name);
					thisChange.put("NEW_VALUE", this.name);
					change.add(thisChange);
				}
				if (!isEqual(this.fullName, room.fullName)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FULL_NAME");
					thisChange.put("OLD_VALUE", room.fullName);
					thisChange.put("NEW_VALUE", this.fullName);
					change.add(thisChange);
				}
				if (!isEqual(this.address, room.address)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ADDRESS");
					thisChange.put("OLD_VALUE", room.address);
					thisChange.put("NEW_VALUE", this.address);
					change.add(thisChange);
				}
				if (!isEqual(this.city, room.city)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "CITY");
					thisChange.put("OLD_VALUE", room.city);
					thisChange.put("NEW_VALUE", this.city);
					change.add(thisChange);
				}
				if (!isEqual(this.state, room.state)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "STATE");
					thisChange.put("OLD_VALUE", room.state);
					thisChange.put("NEW_VALUE", this.state);
					change.add(thisChange);
				}
				if (!isEqual(this.zip, room.zip)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ZIP");
					thisChange.put("OLD_VALUE", room.zip);
					thisChange.put("NEW_VALUE", this.zip);
					change.add(thisChange);
				}
				if (!isEqual(this.country, room.country)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "COUNTRY");
					thisChange.put("OLD_VALUE", room.country);
					thisChange.put("NEW_VALUE", this.country);
					change.add(thisChange);
				}
				if (!isEqual(this.longitude, room.longitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LONGITUDE");
					thisChange.put("OLD_VALUE", room.longitude);
					thisChange.put("NEW_VALUE", this.longitude);
					change.add(thisChange);
				}
				if (!isEqual(this.latitude, room.latitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "LATITUDE");
					thisChange.put("OLD_VALUE", room.latitude);
					thisChange.put("NEW_VALUE", this.latitude);
					change.add(thisChange);
				}
				if (!isEqual(this.altitude, room.altitude)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ALTITUDE");
					thisChange.put("OLD_VALUE", room.altitude);
					thisChange.put("NEW_VALUE", this.altitude);
					change.add(thisChange);
				}
				if (!isEqual(this.building, room.building)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "BUILDING");
					thisChange.put("OLD_VALUE", room.building);
					thisChange.put("NEW_VALUE", this.building);
					change.add(thisChange);
				}
				if (!isEqual(this.floor, room.floor)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "FLOOR");
					thisChange.put("OLD_VALUE", room.floor);
					thisChange.put("NEW_VALUE", this.room);
					change.add(thisChange);
				}
				if (!isEqual(this.room, room.room)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ROOM");
					thisChange.put("OLD_VALUE", room.room);
					thisChange.put("NEW_VALUE", this.room);
					change.add(thisChange);
				}
				if (!isEqual(this.mapsURL, room.mapsURL)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "GOOGLE_MAPS_URL");
					thisChange.put("OLD_VALUE", room.mapsURL);
					thisChange.put("NEW_VALUE", this.mapsURL);
					change.add(thisChange);
				}
				if (!isEqual(this.department, room.department)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "DEPARTMENT");
					thisChange.put("OLD_VALUE", room.department);
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

		public String getRoom() {
			return room;
		}

		public String getMapsURL() {
			return mapsURL;
		}

		public String getDepartment() {
			return department;
		}
	}

	private static void setProperty(Properties properties, String key, String value) {
		if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
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
