package edu.ucdavis.ucdh.stu.stu.batch;

import java.io.PrintWriter;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
 * <p>Compares the real IAM PR Repository database with our copy to see if we need to update anything.</p>
 */
public class IAMUpdateCheck implements SpringBatchJob {
	private static final String[] FIELD_NAME = {"IAMID", "MOTHRAID", "PPSID", "BANNER_PIDM", "STUDENTID", "EXTERNALID", "FIRST_NAME", "LAST_NAME", "MIDDLE_NAME", "PRI_HSAD_ACCOUNT", "KERB_ACCOUNT", "PRI_UCDHS_DEPT_CODE", "PRI_UCDHS_DEPT_NAME", "EMAIL", "IS_EMPLOYEE", "IS_STUDENT", "IS_EXTERNAL", "IS_PREVIOUS_HS_EMPLOYEE", "MODIFY_DATE"};
	private static final String IAM_SQL = "SELECT IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, MODIFY_DATE FROM PR_SERVICES.hs_sd_people ORDER BY IAMID";
//	private static final String IAM_SQL = "SELECT * FROM (SELECT ROWNUM, IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, MODIFY_DATE FROM PR_SERVICES.hs_sd_people) WHERE ROWNUM < 50 ORDER BY IAMID";
	private static final String HS_SQL = "SELECT IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, SYSMODTIME AS MODIFY_DATE FROM IAM WHERE IAMID=?";
	private Log log = LogFactory.getLog(getClass().getName());
	private Connection conn = null;
	private Connection hsConn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private List<String> updateRequired = new ArrayList<String>();
	private String dbDriver = null;
	private String dbURL = null;
	private String dbUser = null;
	private String dbPassword = null;
	private String hsDriver = null;
	private String hsURL = null;
	private String hsUser = null;
	private String hsPassword = null;
	private String outputFileName = null;
	private int recordsRead = 0;
	private int hsRecordsRead = 0;
	private int hsRecordsNotFound = 0;
	private int hsReadFailures = 0;
	private int idsSelected = 0;
	private int idsWritten = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		iamUpdateCheckBegin();
		while (rs.next()) {
			processResultRow();
		}
		return iamUpdateCheckEnd();
	}

	private void iamUpdateCheckBegin() throws Exception {
		log.info("IAMUpdateCheck starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify dbDriver
		if (StringUtils.isEmpty(dbDriver)) {
			throw new IllegalArgumentException("Required property \"dbDriver\" missing or invalid.");
		} else {
			log.info("dbDriver = " + dbDriver);
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
		// verify hsDriver
		if (StringUtils.isEmpty(hsDriver)) {
			throw new IllegalArgumentException("Required property \"hsDriver\" missing or invalid.");
		} else {
			log.info("hsDriver = " + hsDriver);
		}
		// verify hsUser
		if (StringUtils.isEmpty(hsUser)) {
			throw new IllegalArgumentException("Required property \"hsUser\" missing or invalid.");
		} else {
			log.info("hsUser = " + hsUser);
		}
		// verify hsPassword
		if (StringUtils.isEmpty(hsPassword)) {
			throw new IllegalArgumentException("Required property \"hsPassword\" missing or invalid.");
		} else {
			log.info("hsPassword = ********");
		}
		// verify hsURL
		if (StringUtils.isEmpty(hsURL)) {
			throw new IllegalArgumentException("Required property \"hsURL\" missing or invalid.");
		} else {
			log.info("hsURL = " + hsURL);
		}
		// verify outputFileName
		if (StringUtils.isEmpty(outputFileName)) {
			throw new IllegalArgumentException("Required property \"outputFileName\" missing or invalid.");
		} else {
			log.info("outputFileName = " + outputFileName);
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// get IAM data from database
		Class.forName(dbDriver);
		conn = DriverManager.getConnection(dbURL, dbUser, dbPassword);
		stmt = conn.createStatement();
		rs = stmt.executeQuery(IAM_SQL);

		// connect to HS database
		Class.forName(hsDriver);
		hsConn = DriverManager.getConnection(hsURL, hsUser, hsPassword);
	}

	private List<BatchJobServiceStatistic> iamUpdateCheckEnd() throws Exception {
		// close out database connections
		rs.close();
		stmt.close();
		conn.close();
		hsConn.close();

		// write list of selected IDs to text file
		idsSelected = updateRequired.size();
		if (idsSelected > 0) {
			if (log.isDebugEnabled()) {
				log.debug("Wrtitng " + idsSelected + " IDs to file " + outputFileName);
			}
			PrintWriter pw = new PrintWriter(outputFileName);
			Iterator<String> i = updateRequired.iterator();
			while(i.hasNext()) {
				pw.println(i.next());
				idsWritten++;
			}
			pw.close();
			if (log.isDebugEnabled()) {
				log.debug(idsWritten + " IDs written to file " + outputFileName);
			}
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("IAM records read", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsRead + "")));
		stats.add(new BatchJobServiceStatistic("HS records read", BatchJobService.FORMAT_INTEGER, new BigInteger(hsRecordsRead + "")));
		stats.add(new BatchJobServiceStatistic("HS records not found", BatchJobService.FORMAT_INTEGER, new BigInteger(hsRecordsNotFound + "")));
		stats.add(new BatchJobServiceStatistic("Errors reading HS records", BatchJobService.FORMAT_INTEGER, new BigInteger(hsReadFailures + "")));
		stats.add(new BatchJobServiceStatistic("IAM IDs selected", BatchJobService.FORMAT_INTEGER, new BigInteger(idsSelected + "")));
		stats.add(new BatchJobServiceStatistic("IAM IDs written", BatchJobService.FORMAT_INTEGER, new BigInteger(idsWritten + "")));

		// end job
		log.info("IAMUpdateCheck complete.");

		return stats;
	}

	private void processResultRow() throws Exception {
		recordsRead++;
		Map<String,String> iamRecord = buildRecord(rs);
		String iamId = iamRecord.get("IAMID");
		if (log.isDebugEnabled()) {
			log.debug("Processing " + iamRecord.get("FIRST_NAME")  + " " + iamRecord.get("LAST_NAME") + " (" + iamId + ").");
		}
		Map<String,String> hsRecord = fetchHsRecord(iamId);
		if (hsRecord != null) {
			if (StringUtils.isNotEmpty(iamRecord.get("MODIFY_DATE")) && StringUtils.isNotEmpty(hsRecord.get("MODIFY_DATE"))) {
				if (iamRecord.get("MODIFY_DATE").compareTo(hsRecord.get("MODIFY_DATE")) > 0) {
					if (dataDoesNotMatch(iamRecord, hsRecord)) {
						if (log.isDebugEnabled()) {
							log.debug("HS record does not match IAM record; update required.");
						}
						updateRequired.add(iamId);
					} else {
						if (log.isDebugEnabled()) {
							log.debug(iamRecord.get("MODIFY_DATE")  + " is after " + hsRecord.get("MODIFY_DATE") + ", but data is the same in both systems; record will not be selected.");
						}
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug(iamRecord.get("MODIFY_DATE")  + " is before " + hsRecord.get("MODIFY_DATE") + "; record will not be selected.");
					}
				}
			} else {
				log.info("Modify dates missing for " + iamId + "; IAM: " + iamRecord.get("MODIFY_DATE") + "; HS: " + hsRecord.get("MODIFY_DATE"));
				updateRequired.add(iamId);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No matching HS record; update required.");
			}
			updateRequired.add(iamId);
		}
	}

	private Map<String,String> fetchHsRecord(String iamId) throws SQLException {
		Map<String,String> record = null;

		PreparedStatement ps = null;
		ResultSet resultSet = null;
		try {
			ps = hsConn.prepareStatement(HS_SQL);
			ps.setString(1, iamId);
			if (log.isDebugEnabled()) {
				log.debug("Searching for HS record: \"" + iamId + "\".");
			}
			resultSet = ps.executeQuery();
			if (resultSet.next()) {
				hsRecordsRead++;
				record = buildRecord(resultSet);
				if (log.isDebugEnabled()) {
					log.debug("HS record found.");
				}
			} else {
				hsRecordsNotFound++;
				if (log.isDebugEnabled()) {
					log.debug("HS record not found.");
				}
			}
		} catch (Exception e) {
			hsReadFailures++;
			log.error("Exception encountered while attempting to fetch HS record for " + iamId + "; " + e.getMessage(), e);
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					// 
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// 
				}
			}
		}

		return record;
	}

	private boolean dataDoesNotMatch(Map<String,String> iamRecord, Map<String,String> hsRecord) {
		boolean dataDoesNotMatch = false;

		for (int i=0;i<FIELD_NAME.length; i++) {
			String fieldName = FIELD_NAME[i];
			if (!(fieldName.equalsIgnoreCase("MODIFY_DATE"))) {
				if (!(isEqual(iamRecord.get(fieldName), hsRecord.get(fieldName)))) {
					dataDoesNotMatch = true;
					if (log.isDebugEnabled()) {
						log.debug(fieldName + ": " + iamRecord.get(fieldName) + " does not match " + hsRecord.get(fieldName));
					}
				}
			}
		}

		return dataDoesNotMatch;
	}

	private boolean isEqual(String string1, String string2) {
		boolean isEqual = true;

		if (StringUtils.isNotEmpty(string1)) {
			if (StringUtils.isNotEmpty(string2)) {
				if (!(string1.trim().equalsIgnoreCase(string2.trim()))) {
					isEqual = false;
				}
			} else {
				isEqual = false;
			}
		} else {
			if (StringUtils.isNotEmpty(string2)) {
				isEqual = false;
			}
		}

		return isEqual;
	}

	private Map<String,String> buildRecord(ResultSet resultSet) throws SQLException {
		Map<String,String> record = new HashMap<String,String>();

		for (int i=0;i<FIELD_NAME.length; i++) {
			record.put(FIELD_NAME[i], resultSet.getString(FIELD_NAME[i]));
		}

		return record;
	}

	/**
	 * @param dbDriver the dbDriver to set
	 */
	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
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
	 * @param hsDriver the hsDriver to set
	 */
	public void setHsDriver(String hsDriver) {
		this.hsDriver = hsDriver;
	}

	/**
	 * @param hsURL the hsURL to set
	 */
	public void setHsURL(String hsURL) {
		this.hsURL = hsURL;
	}

	/**
	 * @param hsUser the hsUser to set
	 */
	public void setHsUser(String hsUser) {
		this.hsUser = hsUser;
	}

	/**
	 * @param hsPassword the hsPassword to set
	 */
	public void setHsPassword(String hsPassword) {
		this.hsPassword = hsPassword;
	}

	/**
	 * @param outputFileName the outputFileName to set
	 */
	public void setOutputFileName(String outputFileName) {
		this.outputFileName = outputFileName;
	}
}