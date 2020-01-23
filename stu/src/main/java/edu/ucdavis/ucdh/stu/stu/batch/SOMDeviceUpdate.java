package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Scans the SOM PC/Printer database and adds any missing records
 * to the HP Service Manager database.</p>
 */
public class SOMDeviceUpdate implements SpringBatchJob {
	private Log log = LogFactory.getLog(getClass().getName());
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private List<Map<String,String>> devices = new ArrayList<Map<String,String>>();
	private String jdbcDriverName = null;
	private String dbURL = null;
	private String dbUser = null;
	private String dbPassword = null;
	private String dbURLOut = null;
	private String jdbcDriverNameOut = null;
	private String dbUserOut = null;
	private String dbPasswordOut = null;
	private int recordsRead = 0;
	private int recordsFound = 0;
	private int recordsWritten = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		somDeviceUpdateBegin();
		while (rs.next()) {
			processResultRow();
		}
		return somDeviceUpdateEnd();
	}

	private void somDeviceUpdateBegin() throws Exception {
		log.info("SOMDeviceUpdate starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify jdbcDriverName
		if (StringUtils.isEmpty(jdbcDriverName)) {
			throw new IllegalArgumentException("Required property \"jdbcDriverName\" missing or invalid.");
		} else {
			log.info("jdbcDriverName = " + jdbcDriverName);
		}
		// verify dbUser
		if (StringUtils.isEmpty(dbUser)) {
			throw new IllegalArgumentException("Required property \"dbUser\" missing or invalid.");
		} else {
			log.info("dbUser = " + dbUser);
		}
		// verify dbPassword
		if (StringUtils.isEmpty(dbPassword)) {
			throw new IllegalArgumentException("Required property \"dbPassword\" missing or invalid.");
		} else {
			log.info("dbPassword = ********");
		}
		// verify dbURL
		if (StringUtils.isEmpty(dbURL)) {
			throw new IllegalArgumentException("Required property \"dbURL\" missing or invalid.");
		} else {
			log.info("dbURL = " + dbURL);
		}

		// verify jdbcDriverNameOut
		if (StringUtils.isEmpty(jdbcDriverNameOut)) {
			throw new IllegalArgumentException("Required property \"jdbcDriverNameOut\" missing or invalid.");
		} else {
			log.info("jdbcDriverNameOut = " + jdbcDriverNameOut);
		}
		// verify dbUserOut
		if (StringUtils.isEmpty(dbUserOut)) {
			throw new IllegalArgumentException("Required property \"dbUserOut\" missing or invalid.");
		} else {
			log.info("dbUserOut = " + dbUserOut);
		}
		// verify dbPasswordOut
		if (StringUtils.isEmpty(dbPasswordOut)) {
			throw new IllegalArgumentException("Required property \"dbPasswordOut\" missing or invalid.");
		} else {
			log.info("dbPasswordOut = ********");
		}
		// verify dbURLOut
		if (StringUtils.isEmpty(dbURLOut)) {
			throw new IllegalArgumentException("Required property \"dbURLOut\" missing or invalid.");
		} else {
			log.info("dbUrlOut = " + dbURLOut);
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// first gather up all of the data from the SOM database
		Class.forName(jdbcDriverName);
		conn = DriverManager.getConnection(dbURL, dbUser, dbPassword);
		stmt = conn.createStatement();
		rs = stmt.executeQuery("select * from dbo.v_hp_ci order by Computer_ID");
	}

	private List<BatchJobServiceStatistic> somDeviceUpdateEnd() throws Exception {
		rs.close();
		stmt.close();
		conn.close();

		// then check HP to see if any devices need to be added
		try {
			Class.forName(jdbcDriverNameOut);
			conn = DriverManager.getConnection(dbURLOut, dbUserOut, dbPasswordOut);
			stmt = null;
			rs = null;
			Iterator<Map<String,String>> i = devices.iterator();
			while (i.hasNext()) {
				Map<String,String> thisDevice = i.next();
				String id = thisDevice.get("id");
				stmt = conn.createStatement();
				rs = stmt.executeQuery("select * from dbo.DEVICE2M1 where LOGICAL_NAME = 'SOM-" + id + "'");
				if (rs.next()) {
					recordsFound++;
				} else {
					stmt.close();
					PreparedStatement ps = conn.prepareStatement("insert into dbo.DEVICE2M1 (LOGICAL_NAME, CONTACT_NAME, ID, MAC_ADDRESS, ROOM, LOCATION, VENDOR, MANUFACTURER, DEPT, CORP_STRUCTURE, BUILDING, ASSET_TAG, TYPE, ISTATUS, COMMENTS, SYSMODTIME, COMPANY, ASSIGNMENT, OWNER, SUPPORT_GROUPS, CREATED_BY, CREATED_BY_DATE, UCD_DATA_SOURCE) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'computer', 'In use', 'Imported via the SOM device interface', GETDATE(), 'UCDHS', 'SOM Level 2', 'SOM Level 2', 'SOM Level 2', 'SOM device interface', GETDATE(), 'SOM device interface')");
					ps.setString(1, "SOM-" + id);
					ps.setString(2, thisDevice.get("primaryUser"));
					ps.setString(3, thisDevice.get("netBiosName"));
					ps.setString(4, thisDevice.get("mac"));
					ps.setString(5, thisDevice.get("room"));
					ps.setString(6, thisDevice.get("city"));
					ps.setString(7, thisDevice.get("vendor"));
					ps.setString(8, thisDevice.get("vendor"));
					ps.setString(9, thisDevice.get("department"));
					ps.setString(10, thisDevice.get("division"));
					ps.setString(11, thisDevice.get("building"));
					ps.setString(12, thisDevice.get("idTag"));
					if (ps.executeUpdate() > 0) {
						recordsWritten++;
					} else {
						log.error("Unable to insert record for device " + id);
					}
					ps.close();
				}
			}
			rs.close();
			stmt.close();
			conn.close();
		} catch (Exception e) {
			log.error("Exception encountered while posting device data: " + e, e);
			throw new IllegalArgumentException("Exception encountered while posting device data: " + e, e);
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("Records read", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsRead + "")));
		stats.add(new BatchJobServiceStatistic("Records already in HP", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsFound + "")));
		stats.add(new BatchJobServiceStatistic("New records added to HP", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsWritten + "")));

		// end job
		log.info("SOMDeviceUpdate complete.");

		return stats;
	}

	private void processResultRow() throws Exception {
		recordsRead++;
		String id = rs.getString("Computer_ID");
		if (StringUtils.isNotEmpty(id)) {
			Map<String,String> thisDevice = new HashMap<String,String>();
			thisDevice.put("id", id);
			thisDevice.put("primaryUser", rs.getString("PrimaryUser"));
			thisDevice.put("netBiosName", rs.getString("NetBiosName"));
			thisDevice.put("mac", rs.getString("MAC"));
			thisDevice.put("room", rs.getString("Room"));
			thisDevice.put("city", rs.getString("City"));
			thisDevice.put("vendor", rs.getString("Vendor"));
			thisDevice.put("department", rs.getString("DeptName"));
			thisDevice.put("division", rs.getString("Division"));
			thisDevice.put("building", rs.getString("BuildingLong"));
			thisDevice.put("idTag", rs.getString("IDTag"));
			devices.add(thisDevice);
		} else {
			log.info("Record bypassed -- ID is blank");
		}
	}

	/**
	 * @param jdbcDriverName the jdbcDriverName to set
	 */
	public void setJdbcDriverName(String jdbcDriverName) {
		this.jdbcDriverName = jdbcDriverName;
	}

	/**
	 * @param dbURL the dbURL to set
	 */
	public void setDbURL(String dbURL) {
		this.dbURL = dbURL;
	}

	/**
	 * @param dbUser the dbUser to set
	 */
	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}

	/**
	 * @param dbPassword the dbPassword to set
	 */
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	/**
	 * @param dbURLOut the dbURLOut to set
	 */
	public void setDbURLOut(String dbURLOut) {
		this.dbURLOut = dbURLOut;
	}

	/**
	 * @param jdbcDriverNameOut the jdbcDriverNameOut to set
	 */
	public void setJdbcDriverNameOut(String jdbcDriverNameOut) {
		this.jdbcDriverNameOut = jdbcDriverNameOut;
	}

	/**
	 * @param dbUserOut the dbUserOut to set
	 */
	public void setDbUserOut(String dbUserOut) {
		this.dbUserOut = dbUserOut;
	}

	/**
	 * @param dbPasswordOut the dbPasswordOut to set
	 */
	public void setDbPasswordOut(String dbPasswordOut) {
		this.dbPasswordOut = dbPasswordOut;
	}
}
