package edu.ucdavis.ucdh.stu.stu.batch;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.math.BigInteger;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

public class PerformanceMonitor implements SpringBatchJob {
	private static final String PATH_TO_MONITOR = "/util/monitor.xml";
	private static final String INSERT_SQL = "INSERT INTO PM_HISTORY (SERVER_URL, DATE_TIME, CLIENT_SIDE_DURATION, SERVER_SIDE_DURATION, CLEAN_DURATION, INSERT_DURATION, READ_DURATION, UPDATE_DURATION, DELETE_DURATION) VALUES(?, GETDATE(), ?, ?, ?, ?, ?, ?, ?)";
	private Log log = LogFactory.getLog(getClass().getName());
	private Map<String,BigInteger> response = null;
	private HttpClient client = new DefaultHttpClient();
	private DocumentBuilder db = null;
	private Connection conn = null;
	private long start = 0;
	private long end = 0;
	private int maxServerSideDuration = 0;
	private int serverSideDuration = 0;
	private int cleanDuration = 0;
	private int insertDuration = 0;
	private int readDuration = 0;
	private int updateDuration = 0;
	private int deleteDuration = 0;
	private int recordsWritten = 0;
	private int noticesSent = 0;

	// Spring-injected run-time variables
	private DataSource dataSource = null;
	private String notificationService = null;
	private String noticeContext = null;
	private String noticeTemplateId = null;
	private String mailFrom = null;
	private String mailTo = null;
	private String serverURL = null;
	private String alertThreshold = null;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		performanceMonitorBegin();
		processResponse();
		return performanceMonitorEnd();
	}

	private void performanceMonitorBegin() throws Exception {
		// start job
		log.info("PerformanceMonitor starting ...");
		log.info(" ");

		// capture start time
		start = new Date().getTime();

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify dataSource
		if (dataSource == null) {
			throw new IllegalArgumentException("Required property \"dataSource\" missing or invalid.");
		} else {
			log.info("dataSource verified");
		}
		// verify notificationService
		if (StringUtils.isEmpty(notificationService)) {
			throw new IllegalArgumentException("Required property \"notificationService\" missing or invalid.");
		} else {
			log.info("notificationService: " + notificationService);
		}
		// verify noticeContext
		if (StringUtils.isEmpty(noticeContext)) {
			throw new IllegalArgumentException("Required property \"noticeContext\" missing or invalid.");
		} else {
			log.info("noticeContext: " + noticeContext);
		}
		// verify noticeTemplateId
		if (StringUtils.isEmpty(noticeTemplateId)) {
			throw new IllegalArgumentException("Required property \"noticeTemplateId\" missing or invalid.");
		} else {
			log.info("noticeTemplateId: " + noticeTemplateId);
		}
		// verify mailFrom
		if (StringUtils.isEmpty(mailFrom)) {
			throw new IllegalArgumentException("Required property \"mailFrom\" missing or invalid.");
		} else {
			log.info("mailFrom = " + mailFrom);
		}
		// verify mailTo
		if (StringUtils.isEmpty(mailTo)) {
			throw new IllegalArgumentException("Required property \"mailTo\" missing or invalid.");
		} else {
			log.info("mailTo = " + mailTo);
		}
		// verify serverURL
		if (StringUtils.isEmpty(serverURL)) {
			throw new IllegalArgumentException("Required property \"serverURL\" missing or invalid.");
		} else {
			serverURL = serverURL.toLowerCase();
			log.info("serverURL = " + serverURL);
		}
		// verify alertThreshold
		if (StringUtils.isEmpty(alertThreshold)) {
			throw new IllegalArgumentException("Required property \"alertThreshold\" missing.");
		} else {
			maxServerSideDuration = Integer.valueOf(alertThreshold);
			if (!alertThreshold.equals(maxServerSideDuration + "")) {
				throw new IllegalArgumentException("Required property \"alertThreshold\" invalid.");
			} else {
				log.info("alertThreshold = " + maxServerSideDuration);
			}
		}
		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// connect to util database
		conn = dataSource.getConnection();
		log.info("Service Manager database connection established");
		log.info(" ");

		// initialize DocumentBuilder
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		db = dbf.newDocumentBuilder();

		// monitor server
		response = getResponseFromServer();
	}

	private List<BatchJobServiceStatistic> performanceMonitorEnd() throws Exception {
		// capture end time
		end = new Date().getTime();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("Client side duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + (end - start))));
		stats.add(new BatchJobServiceStatistic("Server side duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + serverSideDuration)));
		stats.add(new BatchJobServiceStatistic("Clean duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + cleanDuration)));
		stats.add(new BatchJobServiceStatistic("Insert duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + insertDuration)));
		stats.add(new BatchJobServiceStatistic("Read duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + readDuration)));
		stats.add(new BatchJobServiceStatistic("Update duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + updateDuration)));
		stats.add(new BatchJobServiceStatistic("Delete duration", BatchJobService.FORMAT_DURATION, new BigInteger("" + deleteDuration)));
		stats.add(new BatchJobServiceStatistic("Database records written", BatchJobService.FORMAT_INTEGER, new BigInteger("" + recordsWritten)));
		if (noticesSent > 0) {
			stats.add(new BatchJobServiceStatistic("E-mail alert notifications sent", BatchJobService.FORMAT_INTEGER, new BigInteger(noticesSent + "")));
		}

		// end job
		log.info("PerformanceMonitor complete.");

		return stats;
	}

	private void processResponse() {
		serverSideDuration = getElapsedMs("");
		cleanDuration = getElapsedMs("clean");
		insertDuration = getElapsedMs("insert");
		readDuration = getElapsedMs("read");
		updateDuration = getElapsedMs("update");
		deleteDuration = getElapsedMs("delete");

		PreparedStatement ps = null;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + INSERT_SQL);
			}
			ps = conn.prepareStatement(INSERT_SQL);
			ps.setString(1, serverURL);
			ps.setInt(2, (int) (new Date().getTime() - start));
			ps.setInt(3, serverSideDuration);
			ps.setInt(4, cleanDuration);
			ps.setInt(5, insertDuration);
			ps.setInt(6, readDuration);
			ps.setInt(7, updateDuration);
			ps.setInt(8, deleteDuration);
			recordsWritten = ps.executeUpdate();
			if (log.isDebugEnabled()) {
				log.debug(recordsWritten + " record(s) written to the database");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to insert records into the database: " + e.getMessage(), e);
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}

		if (serverSideDuration > maxServerSideDuration) {
			String message = "Total elapsed milliseconds of " + serverSideDuration + " exceeds the configured threshold of " + maxServerSideDuration + " for monitored server " + serverURL + ".";
			sendAlertNotification(message);
		}
	}

	private Map<String,BigInteger> getResponseFromServer() {
		String url = serverURL + PATH_TO_MONITOR;
		if (log.isDebugEnabled()) {
			log.debug("Opening URL " + url);
		}
		try {
			HttpGet get = new HttpGet(url);
			HttpResponse res = client.execute(get);
			int responseCode = res.getStatusLine().getStatusCode();
			String xml = EntityUtils.toString(res.getEntity());
			if (responseCode == 200) {
				response = new HashMap<String,BigInteger>();
				if (log.isDebugEnabled()) {
					log.debug("XML returned:\n" + xml);
				}
				Document document = db.parse(new InputSource(new StringReader(xml)));
				response.put("start", new BigInteger(getValueByTagName(document, "start")));
				response.put("end", new BigInteger(getValueByTagName(document, "end")));
				response.put("cleanStart", new BigInteger(getValueByTagName(document, "cleanStart")));
				response.put("cleanEnd", new BigInteger(getValueByTagName(document, "cleanEnd")));
				response.put("insertStart", new BigInteger(getValueByTagName(document, "insertStart")));
				response.put("insertEnd", new BigInteger(getValueByTagName(document, "insertEnd")));
				response.put("readStart", new BigInteger(getValueByTagName(document, "readStart")));
				response.put("readEnd", new BigInteger(getValueByTagName(document, "readEnd")));
				response.put("updateStart", new BigInteger(getValueByTagName(document, "updateStart")));
				response.put("updateEnd", new BigInteger(getValueByTagName(document, "updateEnd")));
				response.put("deleteStart", new BigInteger(getValueByTagName(document, "deleteStart")));
				response.put("deleteEnd", new BigInteger(getValueByTagName(document, "deleteEnd")));
			} else {
				if (responseCode == 404) {
					log.error("The performance monitoring servlet is not available.");
				} else {
					log.error("Invalid response from URL " + url + ": " + responseCode);
				}
				String message = "Invalid response from URL \"" + url + "\"; response code is " + responseCode + ".";
				sendAlertNotification(message);
			}
		} catch (Exception e) {
			log.error("Exception occurred processing URL " + url + ": " + e, e);
			String message = "Exception occurred processing URL \"" + url + "\"; the exception is " + e + ".\n\n";
			message += getStackTrace(e);
			sendAlertNotification(message);
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning response: " + response);
		}

		return response;
	}

	private String getValueByTagName(Document document, String tagName) {
		String value = "";

		NodeList nl = document.getElementsByTagName(tagName);
		if (nl != null && nl.getLength() > 0) {
			nl = nl.item(0).getChildNodes();
			if (nl != null && nl.getLength() > 0) {
				if (StringUtils.isNotEmpty(nl.item(0).getNodeValue())) {
					value = nl.item(0).getNodeValue();
				}
			}
		}

		return value;
	}

	private int getElapsedMs(String qualifier) {
		BigInteger value = new BigInteger("0");

		if (response != null) {
			String key = "end";
			if (StringUtils.isNotEmpty(qualifier)) {
				key = qualifier + "End";
			}
			BigInteger endMs = response.get(key);
			key = "start";
			if (StringUtils.isNotEmpty(qualifier)) {
				key = qualifier + "Start";
			}
			BigInteger startMs = response.get(key);
			if (endMs != null && startMs != null) {
				value = new BigInteger("" + (endMs.longValue() - startMs.longValue()));
			}
		}

		return value.intValue();
	}
	private static String getStackTrace(Throwable throwable) {
		final Writer result = new StringWriter();
		final PrintWriter printWriter = new PrintWriter(result);
		throwable.printStackTrace(printWriter);
		return result.toString();
	}

	private void sendAlertNotification(String message) {
		if (log.isDebugEnabled()) {
			log.debug("About to send notice \"" + noticeContext + "/" + noticeTemplateId + "\" to " + mailTo + " from " + mailFrom);
		}
		List<NameValuePair> parameters = new ArrayList<NameValuePair>();
		parameters.add(new BasicNameValuePair("templateId", noticeContext + "/" + noticeTemplateId));
		parameters.add(new BasicNameValuePair("sendFrom", mailFrom));
		parameters.add(new BasicNameValuePair("sendTo", mailTo));
		parameters.add(new BasicNameValuePair("message", message));
		parameters.add(new BasicNameValuePair("serverURL", serverURL));
		try {
			SSLContext ctx = SSLContext.getInstance("TLS");
			X509TrustManager tm = new X509TrustManager() {
				public void checkClientTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}
				public void checkServerTrusted(X509Certificate[] xcs, String string) throws CertificateException {
				}
				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			};
			ctx.init(null, new TrustManager[]{tm}, null);
			SSLSocketFactory ssf = new SSLSocketFactory(ctx, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
			ClientConnectionManager ccm = client.getConnectionManager();
			SchemeRegistry sr = ccm.getSchemeRegistry();
			sr.register(new Scheme("https", 443, ssf));
			client = new DefaultHttpClient(ccm, client.getParams());
			HttpPost post = new HttpPost(notificationService);
			post.setEntity(new UrlEncodedFormEntity(parameters, "UTF-8"));
			HttpResponse response = client.execute(post);
			if (response.getStatusLine().getStatusCode() == 200) {
				log.info("Notice \"" + noticeContext + "/" + noticeTemplateId + "\" sent to " + mailTo + " from " + mailFrom);
				noticesSent++;
			} else {
				log.error("Invalid response received from notification service: " + response.getStatusLine().getStatusCode() + "; " + response.getStatusLine().getReasonPhrase());
				log.error(EntityUtils.toString(response.getEntity()));
			}
		} catch (Exception e) {
			log.error("Exception occurred attempting to send the following alert: \"" + message + "\"; " + e, e);
		}
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * @param notificationService the notificationService to set
	 */
	public void setNotificationService(String notificationService) {
		this.notificationService = notificationService;
	}

	/**
	 * @param noticeContext the noticeContext to set
	 */
	public void setNoticeContext(String noticeContext) {
		this.noticeContext = noticeContext;
	}

	/**
	 * @param noticeTemplateId the noticeTemplateId to set
	 */
	public void setNoticeTemplateId(String noticeTemplateId) {
		this.noticeTemplateId = noticeTemplateId;
	}

	/**
	 * @param mailFrom the mailFrom to set
	 */
	public void setMailFrom(String mailFrom) {
		this.mailFrom = mailFrom;
	}

	/**
	 * @param mailTo the mailTo to set
	 */
	public void setMailTo(String mailTo) {
		this.mailTo = mailTo;
	}

	/**
	 * @param serverURL the serverURL to set
	 */
	public void setServerURL(String serverURL) {
		this.serverURL = serverURL;
	}

	/**
	 * @param alertThreshold the alertThreshold to set
	 */
	public void setAlertThreshold(String alertThreshold) {
		this.alertThreshold = alertThreshold;
	}
}
