package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Posts various statistics from TOC apps to the universal application statistic service.</p>
 */
public class TelephoneIndexRefresh implements SpringBatchJob {
	private static final String DELETE_SQL = "DELETE FROM PHONE_PERSON WHERE SOURCE=?";
	private static final String FIND_ID_SQL = "SELECT PERSON_ID FROM PERSON_ID WHERE IS_ACTIVE='Y' AND ID_TYPE=? AND ID_VALUE=?";
	private static final String INSERT_SQL = "INSERT INTO PHONE_PERSON (PHONE_NR, PERSON_ID, SOURCE, ACTIVE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, 'Y', 0, ?, getdate(), ?)";
	private static final String USER_ID = System.getProperty("user.name");
	private final Log log = LogFactory.getLog(getClass());
	private InetAddress localMachine = null;
	private String localAddress = null;
	private Connection indexConn = null;
	private PreparedStatement indexPs = null;
	private PreparedStatement findIdPs = null;
	private Connection sourceConn = null;
	private PreparedStatement sourcePs = null;
	private ResultSet sourceRs = null;
	private int existingEntriesDeleted = 0;
	private int sourceRecordsRead = 0;
	private int sourceRecordsAccepted = 0;
	private int invalidSourceRecords = 0;
	private int indexEntriesInserted = 0;
	private int failedIndexInserts = 0;
	private int iamIdsFound = 0;
	private int iamIdsNotFound = 0;

	// Spring-injected run-time variables
	private DataSource indexDataSource = null;
	private DataSource sourceDataSource = null;
	private String sourceName = null;
	private String idType = "IAM ID";
	private String sql = null;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		telephoneIndexRefreshBegin();
		while (sourceRs.next()) {
			processTelephoneNumber();
		}
		return telephoneIndexRefreshEnd();
	}

	private void telephoneIndexRefreshBegin() throws Exception {
		log.info("TelephoneIndexRefresh starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify indexDataSource
		if (indexDataSource == null) {
			throw new IllegalArgumentException("Required property \"indexDataSource\" missing or invalid.");
		}
		// verify sourceDataSource
		if (sourceDataSource == null) {
			throw new IllegalArgumentException("Required property \"sourceDataSource\" missing or invalid.");
		}
		// verify sourceName
		if (StringUtils.isNotEmpty(sourceName)) {
			log.info("Source Name: " + sourceName);
		} else {
			throw new IllegalArgumentException("Required property \"sourceName\" missing or invalid.");
		}
		// verify idType
		if (StringUtils.isNotEmpty(idType)) {
			log.info("ID Type: " + idType);
		} else {
			throw new IllegalArgumentException("Required property \"idType\" missing or invalid.");
		}
		// verify sql
		if (StringUtils.isNotEmpty(sql)) {
			log.info("Source SQL: " + sql);
		} else {
			throw new IllegalArgumentException("Required property \"sql\" missing or invalid.");
		}

		// set up local host address
		if (log.isDebugEnabled()) {
			log.debug("Setting up local host address");
		}
		localMachine = InetAddress.getLocalHost();
		if (localMachine != null) {
			localAddress = localMachine.getHostAddress();
		}
		if (log.isDebugEnabled()) {
			log.debug("Local host address is " + localAddress);
		}

		// connect to telephone index database
		if (log.isDebugEnabled()) {
			log.debug("Connecting to telephone index database");
		}
		try {
			indexConn = indexDataSource.getConnection();
			log.info("Connected to telephone index database");
		} catch (Exception e) {
			log.error("Unable to connect to specified telephone index database", e);
			throw new IllegalArgumentException("Unable to connect to specified telephone index database.");
		}

		// connect to source database
		if (log.isDebugEnabled()) {
			log.debug("Connecting to source database");
		}
		try {
			sourceConn = sourceDataSource.getConnection();
			log.info("Connected to source database");
		} catch (Exception e) {
			log.error("Unable to connect to specified source database", e);
			throw new IllegalArgumentException("Unable to connect to specified source database.");
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// prepare findId statement
		findIdPs = indexConn.prepareStatement(FIND_ID_SQL);

		// delete existing entries for this source
		indexPs = indexConn.prepareStatement(DELETE_SQL);
		indexPs.setString(1, sourceName);
		existingEntriesDeleted = indexPs.executeUpdate();

		// prepare insert statement
		indexPs = indexConn.prepareStatement(INSERT_SQL);

		// initiate source sql
		sourcePs = sourceConn.prepareStatement(sql);
		sourceRs = sourcePs.executeQuery();
	}

	private List<BatchJobServiceStatistic> telephoneIndexRefreshEnd() throws Exception {
		// close telephone index database
		try {
			findIdPs.close();
		} catch (Exception e) {
			// no one cares!
		}
		try {
			indexPs.close();
		} catch (Exception e) {
			// no one cares!
		}
		try {
			indexConn.close();
		} catch (Exception e) {
			// no one cares!
		}

		// close source database
		try {
			sourceRs.close();
		} catch (Exception e) {
			// no one cares!
		}
		try {
			sourcePs.close();
		} catch (Exception e) {
			// no one cares!
		}
		try {
			sourceConn.close();
		} catch (Exception e) {
			// no one cares!
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (existingEntriesDeleted > 0) {
			stats.add(new BatchJobServiceStatistic("Existing telephone index entries for this source deleted", BatchJobService.FORMAT_INTEGER, new BigInteger(existingEntriesDeleted + "")));
		}
		if (sourceRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("Source records read", BatchJobService.FORMAT_INTEGER, new BigInteger(sourceRecordsRead + "")));
		}
		if (sourceRecordsAccepted > 0) {
			stats.add(new BatchJobServiceStatistic("Source records accepted", BatchJobService.FORMAT_INTEGER, new BigInteger(sourceRecordsAccepted + "")));
		}
		if (invalidSourceRecords > 0) {
			stats.add(new BatchJobServiceStatistic("Invalid source records", BatchJobService.FORMAT_INTEGER, new BigInteger(invalidSourceRecords + "")));
		}
		if (indexEntriesInserted > 0) {
			stats.add(new BatchJobServiceStatistic("Telephone index entries inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(indexEntriesInserted + "")));
		}
		if (failedIndexInserts > 0) {
			stats.add(new BatchJobServiceStatistic("Failed telephone index entry insert attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedIndexInserts + "")));
		}
		if (iamIdsFound > 0) {
			stats.add(new BatchJobServiceStatistic("IAM IDs found", BatchJobService.FORMAT_INTEGER, new BigInteger(iamIdsFound + "")));
		}
		if (iamIdsNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("IAM IDs not found", BatchJobService.FORMAT_INTEGER, new BigInteger(iamIdsNotFound + "")));
		}

		// end job
		log.info("TelephoneIndexRefresh complete.");

		return stats;
	}

	private void processTelephoneNumber() {
		sourceRecordsRead++;
		String phoneNr = null;
		String iamId = null;
		try {
			phoneNr = fixPhoneNumber(sourceRs.getString(1));
			iamId = getIamId(sourceRs.getString(2));
		} catch (Exception e) {
			log.error("Exception encountered while attempting to read source record: " + e.toString(), e);
		}
		if (log.isDebugEnabled()) {
			log.debug("Processing record \"" + phoneNr + "\" (" + iamId + ")");
		}
		if (StringUtils.isNotEmpty(phoneNr) && StringUtils.isNotEmpty(iamId)) {
			sourceRecordsAccepted++;
			addIndexEntry(phoneNr, iamId);
		} else {
			invalidSourceRecords++;
			log.warn("Bypassing invalid record: \"" + phoneNr + "\" (" + iamId + "). This record will not be processed.");
		}
	}

	private String getIamId(String sourceId) {
		String iamId = null;

		if ("IAM ID".equals(idType)) {
			iamId = sourceId;
		} else {
			try {
				findIdPs.setString(1, idType);
				findIdPs.setString(2, sourceId);
				ResultSet rs = findIdPs.executeQuery();
				if (rs.next()) {
					iamIdsFound++;
					iamId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("IAM ID found for " + idType + " ID \"" + sourceId + "\": " + iamId);
					}
				} else {
					iamIdsNotFound++;
					if (log.isDebugEnabled()) {
						log.debug("IAM ID not found for " + idType + " ID \"" + sourceId + "\".");
					}
				}
			} catch (SQLException e) {
				iamIdsNotFound++;
				log.error("Exception encountered while attempting to find IAM ID for " + idType + " ID \"" + sourceId + "\": " + e.toString(), e);
			}
		}

		return iamId;
	}

	private void addIndexEntry(String phoneNr, String iamId) {
		try {
			indexPs.setString(1, phoneNr);
			indexPs.setString(2, iamId);
			indexPs.setString(3, sourceName);
			indexPs.setString(4, USER_ID);
			indexPs.setString(5, localAddress);
			int count = indexPs.executeUpdate();
			if (count == 1) {
				indexEntriesInserted++;
				if (log.isDebugEnabled()) {
					log.debug("Successfully inserted record \"" + phoneNr + "\" (" + iamId + ").");
				}
			} else {
				failedIndexInserts++;
				log.warn("Record not inserted: \"" + phoneNr + "\" (" + iamId + ").");
			}
		} catch (SQLException e) {
			failedIndexInserts++;
			log.error("Exception encountered while attempting to insert telephone index entry: " + e.toString(), e);
		}
	}

	public String fixPhoneNumber(String string) {
		String response = null;

		if (StringUtils.isNotEmpty(string)) {
			if (log.isDebugEnabled()) {
				log.debug("Fixing phone number: " + string);
			}
			response = string.trim().replaceAll("[^0-9]","");
			if (StringUtils.isNotEmpty(response)) {
				if (response.length() < 10) {
					int missingCharacters = 10 - response.length();
					response = "9167340000".substring(0, missingCharacters) + response;
					if (response.substring(0,6).equals("916733")) {
						response = "916703" + response.substring(6);
					}
				} else if (response.length() > 10) {
					if (response.substring(0,1).equals("1")) {
						response = response.substring(1,11);
					} else {
						response = response.substring(0,10);
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug("Fixed phone number: " + response);
			}
		}

		return response;
	}

	/**
	 * @param indexDataSource the indexDataSource to set
	 */
	public void setIndexDataSource(DataSource indexDataSource) {
		this.indexDataSource = indexDataSource;
	}

	/**
	 * @param sourceDataSource the sourceDataSource to set
	 */
	public void setSourceDataSource(DataSource sourceDataSource) {
		this.sourceDataSource = sourceDataSource;
	}

	/**
	 * @param sourceName the sourceName to set
	 */
	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	/**
	 * @param idType the idType to set
	 */
	public void setIdType(String idType) {
		this.idType = idType;
	}

	/**
	 * @param sql the sql to set
	 */
	public void setSql(String sql) {
		this.sql = sql;
	}
}
