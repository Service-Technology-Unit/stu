package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Checks the IAM Person Repository to see if any of the records is the UCDH Person Repository are no longer there.</p>
 */
public class IAMDeleteCheck implements SpringBatchJob {
	private static final String IAM_SQL = "SELECT IAMID FROM PR_SERVICES.hs_sd_people WHERE IAMID=?";
	private static final String PR_SQL = "SELECT ID, IS_ACTIVE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR FROM PERSON WHERE IS_ACTIVE<>'D' ORDER BY ID";
	private static final String HISTORY_SQL = "INSERT INTO PERSON_HISTORY (PERSON_ID, COLUMN_NAME, OLD_VALUE, NEW_VALUE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES(?, 'IS_ACTIVE', ?, 'D', getdate(), ?, 0, ?)";
	private static final String USER_ID = System.getProperty("user.name");
	private Log log = LogFactory.getLog(getClass().getName());
	private String localAddress = null;
	private Connection iamConn = null;
	private Connection prConn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private DataSource iamDataSource = null;
	private DataSource prDataSource = null;
	private String up2dateService = null;
	private String publisherId = null;
	private List<String> deleted = new ArrayList<String>();
	private HttpClient client = null;
	private int jobId = 9999;
	private int iamRecordsRead = 0;
	private int prRecordsRead = 0;
	private int iamRecordsNotFound = 0;
	private int prRecordsUpdated = 0;
	private int prHistoryRecordsUpdated = 0;
	private int failedPersonUpdates = 0;
	private int deleteActionsPublished = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		iamDeleteCheckBegin();
		while (rs.next()) {
			processResultRow();
		}
		return iamDeleteCheckEnd();
	}

	private void iamDeleteCheckBegin() throws Exception {
		log.info("IAMDeleteCheck starting ...");
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

		// get data from database
		stmt = prConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		rs = stmt.executeQuery(PR_SQL);
	}

	private List<BatchJobServiceStatistic> iamDeleteCheckEnd() throws Exception {
		// close out database connections
		rs.close();
		stmt.close();
		prConn.close();
		iamConn.close();

		// print out deleted IDs
		if (deleted.size() > 0) {
			log.info("Deleted IDs:\n" + StringUtils.join(deleted, "\n"));
		} else {
			log.info("No deleted records discovered during this run.");
		}

		// establish HTTP Client
		client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();

		// publish updates
		if (deleted.size() > 0) {
			log.info("Publishing " + deleted.size() + " DELETE action(s).");
			log.info(" ");
			for (int i=0; i<deleted.size(); i++) {
				String id = deleted.get(i);
				List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
				urlParameters.add(new BasicNameValuePair("_pid", publisherId));
				urlParameters.add(new BasicNameValuePair("_jid", jobId + ""));
				urlParameters.add(new BasicNameValuePair("_action", "delete"));
				urlParameters.add(new BasicNameValuePair("id", id));
				postToService(urlParameters);
				deleteActionsPublished++;
			}
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("Person Repository records read", BatchJobService.FORMAT_INTEGER, new BigInteger(prRecordsRead + "")));
		stats.add(new BatchJobServiceStatistic("IAM records read", BatchJobService.FORMAT_INTEGER, new BigInteger(iamRecordsRead + "")));
		if (iamRecordsNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("IAM records not found", BatchJobService.FORMAT_INTEGER, new BigInteger(iamRecordsNotFound + "")));
		}
		if (prRecordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Person Repository records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(prRecordsUpdated + "")));
		}
		if (prHistoryRecordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Person Repository history records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(prHistoryRecordsUpdated + "")));
		}
		if (failedPersonUpdates > 0) {
			stats.add(new BatchJobServiceStatistic("Failed Person Repository updates", BatchJobService.FORMAT_INTEGER, new BigInteger(failedPersonUpdates + "")));
		}
		if (deleteActionsPublished > 0) {
			stats.add(new BatchJobServiceStatistic("Delete actions published", BatchJobService.FORMAT_INTEGER, new BigInteger(deleteActionsPublished + "")));
		}

		// end job
		log.info("IAMDeleteCheck complete.");

		return stats;
	}

	private void processResultRow() {
		prRecordsRead++;

		String id = "";
		try {
			id = rs.getString("ID");
			if (iamRecordNotFound(id)) {
				String oldValue = rs.getString("IS_ACTIVE");
				rs.updateString("IS_ACTIVE", "D");
				rs.updateTimestamp("SYSMODTIME", new Timestamp(new Date().getTime()));
				rs.updateInt("SYSMODCOUNT", rs.getInt("SYSMODCOUNT") + 1);
				rs.updateString("SYSMODUSER", USER_ID);
				rs.updateString("SYSMODADDR", localAddress);
				rs.updateRow();
				if (log.isDebugEnabled()) {
					log.debug("UCDH Person \"" + id + "\" successfully updated.");
				}
				prRecordsUpdated++;
				updatePersonHistory(id, oldValue);
				deleted.add(id);
			}
		} catch (Exception e) {
			log.error("Exception encountered updating UCDH Person record " + id + ": " + e.getMessage(), e);
			failedPersonUpdates++;
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
	 * <p>Updates the person history for the IS_ACTIVE column value change.</p>
	 *
	 * @param id the IAM ID of the person whose record has been deleted
	 * @param oldValue the original value of the IS_ACTIVE column
	 */
	private void updatePersonHistory(String id, String oldValue) {
		if (log.isDebugEnabled()) {
			log.debug("Updating person history for person " + id);
		}

		PreparedStatement ps = null;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + HISTORY_SQL);
			}
			ps = prConn.prepareStatement(HISTORY_SQL);
			ps.setString(1, id);
			ps.setString(2, oldValue);
			ps.setString(3, USER_ID);
			ps.setString(4, localAddress);
			if (ps.executeUpdate() > 0) {
				prHistoryRecordsUpdated++;
				if (log.isDebugEnabled()) {
					log.debug("Person history log updated for person " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to update person history for person " + id + "; " + e.getMessage(), e);
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

	private void postToService(List<NameValuePair> urlParameters) {
		HttpPost post = new HttpPost(up2dateService);
		String resp = "";
		int rc = 0;
		try {
			post.setEntity(new UrlEncodedFormEntity(urlParameters));
			if (log.isDebugEnabled()) {
				log.debug("Posting to the following URL: " + up2dateService);
				log.debug("Posting the following parameters: " + urlParameters);
			}
			HttpResponse response = client.execute(post);
			rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP Response Code: " + rc);
			}
			if (rc == 200) {
				HttpEntity entity = response.getEntity();
				if (entity != null) {
					resp = EntityUtils.toString(entity);
					if (log.isDebugEnabled()) {
						log.debug("HTTP Response Length: " + resp.length());
						log.debug("HTTP Response: " + resp);
					}
				}
			} else {
				log.error("Invalid response code (" + rc + ") encountered accessing to URL " + up2dateService);
				try {
					resp = EntityUtils.toString(response.getEntity());
				} catch (Exception e) {
					// no one cares
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + up2dateService, e);
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

	public void setUp2dateService(String up2dateService) {
		this.up2dateService = up2dateService;
	}

	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}
}