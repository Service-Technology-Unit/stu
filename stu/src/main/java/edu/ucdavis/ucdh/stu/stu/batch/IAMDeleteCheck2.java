package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Checks the IAM Person Repository to see if any of the records in the UCDH copy are no longer there.</p>
 */
public class IAMDeleteCheck2 implements SpringBatchJob {
	private static final String IAM_SQL = "SELECT IAMID FROM PR_SERVICES.hs_sd_people WHERE IAMID=?";
	private static final String UCDH_SQL = "SELECT IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR FROM IAM ORDER BY IAMID";
	private static final String INSERT_SQL = "INSERT INTO IAM_DELETED (IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String DELETE_SQL = "DELETE FROM IAM WHERE IAMID=?";
	private Log log = LogFactory.getLog(getClass().getName());
	private Connection iamConn = null;
	private Connection prConn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private DataSource iamDataSource = null;
	private DataSource prDataSource = null;
	private List<String> deleted = new ArrayList<String>();
	private int iamRecordsRead = 0;
	private int prRecordsRead = 0;
	private int iamRecordsNotFound = 0;
	private int ucdhRecordsDeleted = 0;
	private int ucdhArchiveRecordsInserted = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		IAMDeleteCheck2Begin();
		while (rs.next()) {
			processResultRow();
		}
		return IAMDeleteCheck2End();
	}

	private void IAMDeleteCheck2Begin() throws Exception {
		log.info("IAMDeleteCheck2 starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify iamDataSource
		if (iamDataSource == null) {
			throw new IllegalArgumentException("Required property \"iamDataSource\" missing or invalid.");
		} else {
			iamConn = iamDataSource.getConnection();
			log.info("iamDataSource validated.");
		}

		// verify prDataSource
		if (prDataSource == null) {
			throw new IllegalArgumentException("Required property \"prDataSource\" missing or invalid.");
		} else {
			prConn = prDataSource.getConnection();
			log.info("prDataSource validated.");
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// get data from database
		stmt = prConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		rs = stmt.executeQuery(UCDH_SQL);
	}

	private List<BatchJobServiceStatistic> IAMDeleteCheck2End() throws Exception {
		// close out database connections
		rs.close();
		stmt.close();
		iamConn.close();

		// print out deleted IDs
		if (deleted.size() > 0) {
			Iterator<String> i = deleted.iterator();
			while (i.hasNext()) {
				deleteUCDHRecord(i.next());
			}
			log.info("Deleted IDs:\n" + StringUtils.join(deleted, "\n"));
		} else {
			log.info("No deleted records discovered during this run.");
		}
		prConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("Person Repository records read", BatchJobService.FORMAT_INTEGER, new BigInteger(prRecordsRead + "")));
		stats.add(new BatchJobServiceStatistic("IAM records read", BatchJobService.FORMAT_INTEGER, new BigInteger(iamRecordsRead + "")));
		if (iamRecordsNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("IAM records not found", BatchJobService.FORMAT_INTEGER, new BigInteger(iamRecordsNotFound + "")));
		}
		if (ucdhRecordsDeleted > 0) {
			stats.add(new BatchJobServiceStatistic("UCDH IAM records deleted", BatchJobService.FORMAT_INTEGER, new BigInteger(ucdhRecordsDeleted + "")));
		}
		if (ucdhArchiveRecordsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("UCDH IAM Archive records inserted", BatchJobService.FORMAT_INTEGER, new BigInteger(ucdhArchiveRecordsInserted + "")));
		}

		// end job
		log.info("IAMDeleteCheck2 complete.");

		return stats;
	}

	private void processResultRow() {
		prRecordsRead++;

		String id = "";
		try {
			id = rs.getString("IAMID");
			if (iamRecordNotFound(id)) {
				insertIAMArchiveRecord(id, rs);
				deleted.add(id);
			}
		} catch (Exception e) {
			log.error("Exception encountered processing IAM record " + id + ": " + e.getMessage(), e);
		}
	}

	/**
	 * <p>Searches the IAM Person Repository for the specified IAM ID and returns true if the record is not found.</p>
	 *
	 * @param id the IAM ID of the person whose record has been deleted
	 * @return true if the record is not found
	 */
	private boolean iamRecordNotFound(String id)  {
		boolean notFound = false;

		PreparedStatement ps = null;
		ResultSet resultSet = null;
		try {
			ps = iamConn.prepareStatement(IAM_SQL);
						ps.setString(1, id);
			if (log.isDebugEnabled()) {
				log.debug("Searching for IAM record: \"" + id + "\".");
			}
			resultSet = ps.executeQuery();
			if (resultSet.next()) {
				iamRecordsRead++;
			} else {
				iamRecordsNotFound++;
				notFound = true;
				if (log.isDebugEnabled()) {
					log.debug("IAM record not found.");
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to fetch IAM record for " + id + "; " + e.getMessage(), e);
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

		return notFound;
	}

	/**
	 * <p>Creates an archive record of the data that is about to be deleted.</p>
	 *
	 * @param id the IAM ID of the person whose record should be deleted
	 * @param rs the original data in the UCDH IAM table
	 */
	private void insertIAMArchiveRecord(String id, ResultSet rs) {
		if (log.isDebugEnabled()) {
			log.debug("Creating archive record for person " + id);
		}

		PreparedStatement ps = null;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + INSERT_SQL);
			}
			ps = prConn.prepareStatement(INSERT_SQL);
			ps.setString(1, id);
			ps.setString(2, rs.getString("MOTHRAID"));
			ps.setString(3, rs.getString("PPSID"));
			ps.setString(4, rs.getString("BANNER_PIDM"));
			ps.setString(5, rs.getString("STUDENTID"));
			ps.setString(6, rs.getString("EXTERNALID"));
			ps.setString(7, rs.getString("FIRST_NAME"));
			ps.setString(8, rs.getString("LAST_NAME"));
			ps.setString(9, rs.getString("MIDDLE_NAME"));
			ps.setString(10, rs.getString("PRI_HSAD_ACCOUNT"));
			ps.setString(11, rs.getString("KERB_ACCOUNT"));
			ps.setString(12, rs.getString("PRI_UCDHS_DEPT_CODE"));
			ps.setString(13, rs.getString("PRI_UCDHS_DEPT_NAME"));
			ps.setString(14, rs.getString("EMAIL"));
			ps.setString(15, rs.getString("IS_EMPLOYEE"));
			ps.setString(16, rs.getString("IS_STUDENT"));
			ps.setString(17, rs.getString("IS_EXTERNAL"));
			ps.setString(18, rs.getString("IS_PREVIOUS_HS_EMPLOYEE"));
			ps.setString(19, rs.getString("SYSMODCOUNT"));
			ps.setString(20, rs.getString("SYSMODUSER"));
			ps.setString(21, rs.getString("SYSMODTIME"));
			ps.setString(22, rs.getString("SYSMODADDR"));
			if (ps.executeUpdate() > 0) {
				ucdhArchiveRecordsInserted++;
				if (log.isDebugEnabled()) {
					log.debug("Archive record inserted for person " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to insert archive record for person " + id + "; " + e.getMessage(), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}
	}

	/**
	 * <p>Deletes the UCDH copy of the IAM record.</p>
	 *
	 * @param id the IAM ID of the person whose record should be deleted
	 */
	private void deleteUCDHRecord(String id) {
		if (log.isDebugEnabled()) {
			log.debug("Deleting UCDH IAM record for person " + id);
		}

		PreparedStatement ps = null;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + DELETE_SQL);
			}
			ps = prConn.prepareStatement(DELETE_SQL);
			ps.setString(1, id);
			if (ps.executeUpdate() > 0) {
				ucdhRecordsDeleted++;
				if (log.isDebugEnabled()) {
					log.debug("IAM record deleted for person " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to delete UCDH IAM record for person " + id + "; " + e.getMessage(), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}
	}

	/**
	 * @param prDataSource the prDataSource to set
	 */
	public void setPrDataSource(DataSource prDataSource) {
		this.prDataSource = prDataSource;
	}

	/**
	 * @param iamDataSource the iamDataSource to set
	 */
	public void setIamDataSource(DataSource iamDataSource) {
		this.iamDataSource = iamDataSource;
	}
}