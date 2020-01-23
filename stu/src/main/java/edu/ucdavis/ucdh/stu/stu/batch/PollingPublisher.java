package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

public class PollingPublisher implements SpringBatchJob {
	private static final long ONE_HOUR = 60 * 60 * 1000;
	private static final String CHECK_LOG_SQL = "SELECT SYSMODTIME FROM UPDATELOG WHERE IAM_ID=? AND LAST_UPDATE=?";
	private static final String UPDATE_LOG_SQL = "INSERT INTO UPDATELOG (IAM_ID, LAST_UPDATE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES (?, ?, getdate(), 'up2date', 0, ?)";
	private static final String FETCH_DATA_SQL = "SELECT ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, DEPT_NAME, HS_AD_ID, HS_AD_ID_CT, KERBEROS_ID, KERBEROS_ID_CT, PPS_ID, PPS_ID_CT, EXTERNAL_ID, EXTERNAL_ID_CT, MOTHRA_ID, MOTHRA_ID_CT, BANNER_ID, BANNER_ID_CT, STUDENT_ID, STUDENT_ID_CT, VOLUNTEER_ID, VOLUNTEER_ID_CT, IS_ACTIVE, IS_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, LOCATION_NAME, ADDRESS, CITY, STATE, ZIP, BUILDING, FLOOR, ROOM, CUBE FROM VIEW_PERSON_ALL WHERE ID=?";
	private final Log log = LogFactory.getLog(getClass());
	private InetAddress localMachine = null;
	private String localAddress = null;
	private Connection conn = null;
	private Connection logConn = null;
	private PreparedStatement ps = null;
	private ResultSet rs = null;
	private String srcDriver = null;
	private String srcURL = null;
	private String srcUser = null;
	private String srcPassword = null;
	private String logDriver = null;
	private String logURL = null;
	private String logUser = null;
	private String logPassword = null;
	private String publisherId = null;
	private Up2DateService up2dateService = null;
	private String srcTableName = null;
	private String srcTimestampColumn = null;
	private Date srcLastPolled = null;
	private List<Map<String, String>> field = new ArrayList<Map<String, String>>();
	private int publisherRecordsObtained = 0;
	private int publisherRecordsUpdated = 0;
	private int sourceSystemRecordsRead = 0;
	private int up2dateServiceCalls = 0;
	private int recordsAlreadyProcessed = 0;
	private int updateLogRecordsRead = 0;
	private int existingRecordsRead = 0;
	private int unchangedRecordsIgnored = 0;
	private int updateLogRecordsWritten = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		pollingPublisherBegin();
		while (rs.next()) {
			processUpdate();
		}
		return pollingPublisherEnd();
	}

	private void pollingPublisherBegin() throws Exception {
		String[] fieldList;
		log.info("PollingPublisher starting ...");
		log.info(" ");
		log.info("Validating run time properties ...");
		log.info(" ");

		// srcDriver
		if (StringUtils.isEmpty(srcDriver)) {
			throw new IllegalArgumentException("Required property \"srcDriver\" missing or invalid.");
		}
		log.info("srcDriver = " + srcDriver);

		// srcUser
		if (StringUtils.isEmpty(srcUser)) {
			throw new IllegalArgumentException("Required property \"srcUser\" missing or invalid.");
		}

		// srcUser
		log.info("srcUser = " + srcUser);
		if (StringUtils.isEmpty(srcPassword)) {
			throw new IllegalArgumentException("Required property \"srcPassword\" missing or invalid.");
		}
		log.info("srcPassword = ********");

		// srcURL
		if (StringUtils.isEmpty(srcURL)) {
			throw new IllegalArgumentException("Required property \"srcURL\" missing or invalid.");
		}
		log.info("srcURL = " + srcURL);

		// srcDriver
		if (StringUtils.isEmpty(srcDriver)) {
			throw new IllegalArgumentException("Required property \"srcDriver\" missing or invalid.");
		}
		log.info("logDriver = " + logDriver);

		// logUser
		if (StringUtils.isEmpty(logUser)) {
			throw new IllegalArgumentException("Required property \"logUser\" missing or invalid.");
		}
		log.info("logUser = " + logUser);

		// logPassword
		if (StringUtils.isEmpty(logPassword)) {
			throw new IllegalArgumentException("Required property \"logPassword\" missing or invalid.");
		}
		log.info("logPassword = ********");

		// logURL
		if (StringUtils.isEmpty(logURL)) {
			throw new IllegalArgumentException("Required property \"logURL\" missing or invalid.");
		}
		log.info("logURL = " + logURL);

		// publisherId
		if (StringUtils.isEmpty(publisherId)) {
			throw new IllegalArgumentException("Required property \"publisherId\" missing or invalid.");
		}
		log.info("publisherId = " + publisherId);
		if (up2dateService == null) {
			throw new IllegalArgumentException("Required property \"up2dateService\" missing or invalid.");
		}
		log.info("Up2Date service validated.");
		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// set up local host address
		if (log.isDebugEnabled()) {
			log.debug("Setting up local host address");
		}
		localMachine = InetAddress.getLocalHost();
		if (localMachine != null) {
			localAddress = localMachine.getHostAddress();
		}
		log.info("Local host address is " + localAddress);
		log.info(" ");

		// fetch publisher information
		Properties publisher = up2dateService.getPublisher(publisherId);
		if (publisher == null) {
			throw new IllegalArgumentException("There is no publisher on file with an ID of " + publisherId + ".");
		}
		publisherRecordsObtained++;
		srcTableName = publisher.getProperty("tableName");
		srcTimestampColumn = publisher.getProperty("timestampColumn");
		String lastPolled = publisher.getProperty("lastPolled");
		if (StringUtils.isNotEmpty(lastPolled)) {
			srcLastPolled = new Date(Long.parseLong(publisher.getProperty("lastPolled")));
		}
		if ((fieldList = publisher.getProperty("field").split(";")).length <= 0) throw new IllegalArgumentException("There are no source system fields on file for publisher " + publisherId + ".");
		
		for (int i=0; i<fieldList.length; i++) {
			HashMap<String, String> thisField = new HashMap<String, String>();
			String[] parts = fieldList[i].split(",");
			thisField.put("fieldName", parts[0]);
			thisField.put("columnName", parts[1]);
			field.add(thisField);
		}
		log.info("Publisher data obtained for publisher " + publisherId + "; data fields: " + field);
		log.info(" ");

		// connect to update log database
		Class.forName(logDriver);
		logConn = DriverManager.getConnection(logURL, logUser, logPassword);
		log.info("Connection established to update log database");
		log.info(" ");

		// initiate source database query
		String sql = "SELECT ";
		sql += srcTimestampColumn;
		sql += " AS LAST_UPDATE_DATE_TIME";
		Iterator<Map<String, String>> i2 = field.iterator();
		while (i2.hasNext()) {
			sql += ", ";
			sql += i2.next().get("columnName");
		}
		sql += " FROM ";
		sql += srcTableName;
		if (srcLastPolled != null) {
			sql += " WHERE ";
			sql += srcTimestampColumn;
			sql += " > ?";
		}
		sql += " ORDER BY LAST_UPDATE_DATE_TIME";
		Class.forName(srcDriver);
		conn = DriverManager.getConnection(srcURL, srcUser, srcPassword);
		ps = conn.prepareStatement(sql);
		Timestamp searchFrom = null;
		if (srcLastPolled != null) {
			searchFrom = new Timestamp(srcLastPolled.getTime() - (3 * ONE_HOUR));
			ps.setTimestamp(1, searchFrom);
		}
		if (log.isDebugEnabled()) {
			log.debug("Using SQL: " + sql);
			if (searchFrom != null) {
				log.debug("Searching for updates since " + searchFrom);
			}
		}
		rs = ps.executeQuery();
		log.info("Connection established to source system database");
		log.info(" ");
	}

	private List<BatchJobServiceStatistic> pollingPublisherEnd() throws Exception {
		// close connections
		rs.close();
		ps.close();
		conn.close();
		logConn.close();

		// update publisher
		if (sourceSystemRecordsRead > 0) {
			if (up2dateService.updatePublisher(publisherId, srcLastPolled)) {
				publisherRecordsUpdated++;
				if (log.isDebugEnabled()) {
					log.debug("Last polled date/time reset to " + srcLastPolled);
				}
			} else {
				log.error("Unable to update publisher with last polled date/time");
			}
		}

		// report run statistics
		ArrayList<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (publisherRecordsObtained > 0) {
			stats.add(new BatchJobServiceStatistic("Up2Date publisher information accessed", "integer", new BigInteger(String.valueOf(publisherRecordsObtained))));
		}
		if (sourceSystemRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Source system records read", "integer", new BigInteger(String.valueOf(sourceSystemRecordsRead))));
		}
		if (updateLogRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Update log records read", "integer", new BigInteger(String.valueOf(updateLogRecordsRead))));
		}
		if (existingRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Existing master records read", "integer", new BigInteger(String.valueOf(existingRecordsRead))));
		}
		if (recordsAlreadyProcessed > 0) {
			stats.add(new BatchJobServiceStatistic("Previously processed records ignored", "integer", new BigInteger(String.valueOf(recordsAlreadyProcessed))));
		}
		if (unchangedRecordsIgnored > 0) {
			stats.add(new BatchJobServiceStatistic("Unchanged records ignored", "integer", new BigInteger(String.valueOf(unchangedRecordsIgnored))));
		}
		if (up2dateServiceCalls > 0) {
			stats.add(new BatchJobServiceStatistic("Calls made to the Up2Date service", "integer", new BigInteger(String.valueOf(up2dateServiceCalls))));
		}
		if (updateLogRecordsWritten > 0) {
			stats.add(new BatchJobServiceStatistic("Update log records written", "integer", new BigInteger(String.valueOf(updateLogRecordsWritten))));
		}
		if (publisherRecordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Up2Date publisher information updated", "integer", new BigInteger(String.valueOf(publisherRecordsUpdated))));
		}
		log.info("PollingPublisher complete.");

		return stats;
	}

	private void processUpdate() throws Exception {
		sourceSystemRecordsRead++;
		Timestamp lastPolled = rs.getTimestamp("LAST_UPDATE_DATE_TIME");
		if (lastPolled != null) {
			srcLastPolled = new Date(lastPolled.getTime());
			if (log.isDebugEnabled()) {
				log.debug("srcLastPolled now " + srcLastPolled);
			}
		}
		Properties properties = new Properties();
		for (Map<String, String> thisField : field) {
			String fieldName = thisField.get("fieldName");
			String columnName = thisField.get("columnName");
			String fieldValue = rs.getString(columnName);
			if (StringUtils.isEmpty(fieldValue)) {
				fieldValue = "";
			}
			properties.setProperty(fieldName, fieldValue);
		}
		String iamId = properties.getProperty("id");
		String lastUpdate = rs.getString("LAST_UPDATE_DATE_TIME");
		if (alreadyProcessed(iamId, lastUpdate)) {
			recordsAlreadyProcessed++;
			if (log.isDebugEnabled()) {
				log.debug("Ignoring record " + iamId + " last updated " + lastUpdate + ", as it has already been processed.");
			}
		} else if (dataUnchanged(iamId, properties)) {
			unchangedRecordsIgnored++;
			if (log.isDebugEnabled()) {
				log.debug("Ignoring record " + iamId + " last updated " + lastUpdate + ", as the data has not changed.");
			}
		} else {
			if (log.isDebugEnabled()) {
				for (Map<String, String> thisField : field) {
					String fieldName = thisField.get("fieldName");
					String columnName = thisField.get("columnName");
					String fieldValue = properties.getProperty(fieldName);
					log.debug("Field: " + fieldName + "; Column: " + columnName + "; Value: " + fieldValue);
				}
			}
			up2dateService.post(new Update(publisherId, "change", properties));
			up2dateServiceCalls++;
		}
	}

	private boolean alreadyProcessed(String iamId, String lastUpdate) {
		boolean response = false;

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = logConn.prepareStatement(CHECK_LOG_SQL);
			ps.setString(1, iamId);
			ps.setString(2, lastUpdate);
			if (log.isDebugEnabled()) {
				log.debug("Using SQL: " + CHECK_LOG_SQL);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				updateLogRecordsRead++;
				response = true;
			} else {
				try {
					ps.close();
				} catch (Exception e) {
					
				}
				ps = logConn.prepareStatement(UPDATE_LOG_SQL);
				ps.setString(1, iamId);
				ps.setString(2, lastUpdate);
				ps.setString(3, localAddress);
				if (log.isDebugEnabled()) {
					log.debug("Using SQL: " + UPDATE_LOG_SQL);
				}
				if (ps.executeUpdate() == 1) {
					updateLogRecordsWritten++;
				} else {
					log.error("Unable to update log database for IAM ID " + iamId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing update log database: " + e, e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				
			}
			try {
				ps.close();
			} catch (Exception e) {
				
			}
		}
		return response;
	}

	private boolean dataUnchanged(String iamId, Properties properties) {
		boolean response = false;

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = logConn.prepareStatement(FETCH_DATA_SQL);
			ps.setString(1, iamId);
			if (log.isDebugEnabled()) {
				log.debug("Using SQL: " + FETCH_DATA_SQL);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				existingRecordsRead++;
				if (log.isDebugEnabled()) {
					log.debug("Existing record found for IAM ID " + iamId);
				}
				if (isEqual(properties.getProperty("bannerId"), rs.getString("BANNER_ID")) &&
						isEqual(properties.getProperty("email"), rs.getString("EMAIL")) &&
						isEqual(properties.getProperty("externalId"), rs.getString("EXTERNAL_ID")) &&
						isEqual(properties.getProperty("firstName"), rs.getString("FIRST_NAME")) &&
						isEqual(properties.getProperty("isEmployee"), rs.getString("IS_EMPLOYEE")) &&
						isEqual(properties.getProperty("isExternal"), rs.getString("IS_EXTERNAL")) &&
						isEqual(properties.getProperty("isPrevEmployee"), rs.getString("IS_PREVIOUS_HS_EMPLOYEE")) &&
						isEqual(properties.getProperty("isStudent"), rs.getString("IS_STUDENT")) &&
						isEqual(properties.getProperty("kerberosId"), rs.getString("KERBEROS_ID")) &&
						isEqual(properties.getProperty("lastName"), rs.getString("LAST_NAME")) &&
						isEqual(properties.getProperty("middleName"), rs.getString("MIDDLE_NAME")) &&
						isEqual(properties.getProperty("mothraId"), rs.getString("MOTHRA_ID")) &&
						isEqual(properties.getProperty("ppsId"), rs.getString("PPS_ID")) &&
						isEqual(properties.getProperty("adId"), rs.getString("HS_AD_ID")) &&
						isEqual(properties.getProperty("deptId"), rs.getString("DEPT_ID")) &&
						isEqual(properties.getProperty("deptName"), rs.getString("DEPT_NAME")) &&
						isEqual(properties.getProperty("studentId"), rs.getString("STUDENT_ID"))) {
					response = true;
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing existing data: " + e, e);
		} finally {
			try {
				rs.close();
			} catch (Exception e) {
				
			}
			try {
				ps.close();
			} catch (Exception e) {
				
			}
		}

		return response;
	}

	private boolean isEqual(String s1, String s2) {
		boolean eq = true;

		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.trim().equalsIgnoreCase(s2.trim());
			} else {
				eq = false;
			}
		} else {
			if (StringUtils.isNotEmpty(s2)) {
				eq = false;
			}
		}

		return eq;
	}

	public void setSrcDriver(String srcDriver) {
		this.srcDriver = srcDriver;
	}

	public void setSrcURL(String srcURL) {
		this.srcURL = srcURL;
	}

	public void setSrcUser(String srcUser) {
		this.srcUser = srcUser;
	}

	public void setSrcPassword(String srcPassword) {
		this.srcPassword = srcPassword;
	}

	public void setLogDriver(String logDriver) {
		this.logDriver = logDriver;
	}

	public void setLogURL(String logURL) {
		this.logURL = logURL;
	}

	public void setLogUser(String logUser) {
		this.logUser = logUser;
	}

	public void setLogPassword(String logPassword) {
		this.logPassword = logPassword;
	}

	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}

	public void setUp2dateService(Up2DateService up2dateService) {
		this.up2dateService = up2dateService;
	}
}