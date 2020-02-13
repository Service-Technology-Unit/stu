package edu.ucdavis.ucdh.stu.stu.batch;

import java.io.StringReader;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

public class LocationDataEmail implements SpringBatchJob {
	private static final String SQL = "select Bldg.UCDHS_Bldg_Num, Bldg.Primary_Display_Name, Bldg.Asset_Number, Floor.Floor_Code, Floor.Floor_Name, Suite, Space, Room_Share_Use_Name, Dept.cost_center, Dept.dept_name from DeptRoom left outer join Dept on Dept.Space_Acctng_Dept_Key=DeptRoom.Space_Acctng_Dept_Key left outer join Room on Room.Bldg_Key=DeptRoom.Bldg_Key and Room.Floor_Key=DeptRoom.Floor_Key and Room.Room_Key=DeptRoom.Room_Key left outer join Floor on Floor.Bldg_Key=DeptRoom.Bldg_Key and Floor.Floor_Key=DeptRoom.Floor_Key left outer join Building as Bldg on Bldg.Bldg_Key=DeptRoom.Bldg_Key where Dept.cost_center > '' and Bldg.UCDHS_Bldg_Num > '' order by Bldg.UCDHS_Bldg_Num, Floor.Floor_Order, Space, Dept.cost_center";
	private static final String LF = System.getProperty("line.separator");
	private static final String[] OWNERS = {"AUD","BKS","CAM","FIR","GSM","KFH","PCN","POL","UMC"};
	private Log log = LogFactory.getLog(getClass().getName());
	private Statement stmt = null;
	private ResultSet rs = null;
	private Connection conn = null;
	private Session session = null;
	private InternetAddress fromAddress = null;
	private List<InternetAddress> toAddresses = new ArrayList<InternetAddress>();
	private List<InternetAddress> mail2ToAddresses = new ArrayList<InternetAddress>();
	private List<String> validOwners = new ArrayList<String>();
	private String csvFileName = null;
	private String csvData = "\"Building ID\",\"Building\",\"Asset Nr\",\"Floor Code\",\"Floor\",\"Suite\",\"Room\",\"Usage\",\"Cost Center Text\",\"Department\",\"Cost Center ID\",\"Cost Center Name\",\"Account ID\",\"Org ID\",\"Manager ID\",\"Manager\"" + LF;
	private Map<String,Map<String,String>> cc = new HashMap<String,Map<String,String>>();
	private Map<String,String> noCc = new HashMap<String,String>();
	private HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
	private DocumentBuilder db = null;
	private int readCt = 0;
	private int writeCt = 0;
	private int emailCt = 0;
	private int email2Ct = 0;

	// Spring-injected run-time variables
	private DataSource dataSource = null;
	private String mailServer = null;
	private String mailFrom = null;
	private String mailTo = null;
	private String mail2To = null;
	private String mailSubject = null;
	private String mail2Subject = null;
	private String ccServiceURL = null;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		locationDataEmailBegin();
		while (rs.next()) {
			processResultRow();
		}
		return locationDataEmailEnd();
	}

	private void locationDataEmailBegin() throws Exception {
		// start job
		log.info("LocationDataEmail starting ...");
		log.info(" ");

		// connect to input database
		conn = dataSource.getConnection();
		log.info("Service Manager database connection established");
		log.info(" ");

		// establish e-mail session
		Properties props = System.getProperties();
		props.put("mail.smtp.host", mailServer);
		session = Session.getDefaultInstance(props, null);
		log.info("Connection established to SMTP mail server: " + mailServer);
		log.info(" ");

		// set up e-mail addresses
		fromAddress = new InternetAddress(mailFrom);
		String[] recipient = mailTo.split(",");
		for (int i=0; i<recipient.length; i++) {
			if (StringUtils.isNotEmpty(recipient[i])) {
				toAddresses.add(new InternetAddress(recipient[i]));
			}
		}
		if (toAddresses.size() == 0) {
			throw new IllegalArgumentException("No valid recipient addresses specified");
		}
		recipient = mail2To.split(",");
		for (int i=0; i<recipient.length; i++) {
			if (StringUtils.isNotEmpty(recipient[i])) {
				mail2ToAddresses.add(new InternetAddress(recipient[i]));
			}
		}
		log.info("From and To e-mail addresses verified");
		log.info(" ");

		// set up file name
		Date rightNow = new Date();
		DateFormat format = new SimpleDateFormat("yyyyMM");
		csvFileName = "location-" + format.format(rightNow) + ".csv";
		log.info("The file name used for this run will be " + csvFileName);
		log.info(" ");

		// initialize DocumentBuilder
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		db = dbf.newDocumentBuilder();

		// initialize owner list
		for (int i=0; i<OWNERS.length; i++) {
			validOwners.add(OWNERS[i]);
		}

		// fetch input data
		stmt = conn.createStatement();
		rs = stmt.executeQuery(SQL);
		log.info("Database query executed");
		log.info(" ");
	}

	private List<BatchJobServiceStatistic> locationDataEmailEnd() throws Exception {
		// close connection to databases
		conn.close();

		// send out e-mail
		Iterator<InternetAddress> i = toAddresses.iterator();
		while (i.hasNext()) {
			sendEmail(i.next());
		}

		// send out 2nd e-mail
		if (noCc.keySet().size() > 0) {
			i = mail2ToAddresses.iterator();
			while (i.hasNext()) {
				send2ndEmail(i.next());
			}
		}

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		stats.add(new BatchJobServiceStatistic("Records read", BatchJobService.FORMAT_INTEGER, new BigInteger(readCt + "")));
		stats.add(new BatchJobServiceStatistic("Records written", BatchJobService.FORMAT_INTEGER, new BigInteger(writeCt + "")));
		stats.add(new BatchJobServiceStatistic("Cost Centers found", BatchJobService.FORMAT_INTEGER, new BigInteger(cc.keySet().size() + "")));
		stats.add(new BatchJobServiceStatistic("Cost Centers not found", BatchJobService.FORMAT_INTEGER, new BigInteger(noCc.keySet().size() + "")));
		stats.add(new BatchJobServiceStatistic("E-mail messages sent", BatchJobService.FORMAT_INTEGER, new BigInteger(emailCt + "")));
		stats.add(new BatchJobServiceStatistic("2nd E-mail messages sent", BatchJobService.FORMAT_INTEGER, new BigInteger(email2Ct + "")));

		// end job
		log.info("LocationDataEmail complete.");

		return stats;
	}

	private void processResultRow() throws Exception {
		readCt++;
		csvData += "\"" + getValue("UCDHS_Bldg_Num") + "\",";
		csvData += "\"" + getValue("Primary_Display_Name") + "\",";
		csvData += "\"" + getValue("Asset_Number") + "\",";
		csvData += "\"" + getValue("Floor_Code") + "\",";
		csvData += "\"" + getValue("Floor_Name") + "\",";
		csvData += "\"" + getValue("Suite") + "\",";
		csvData += "\"" + getValue("Space") + "\",";
		csvData += "\"" + getValue("Room_Share_Use_Name") + "\",";
		csvData += "\"" + getValue("cost_center") + "\",";
		csvData += "\"" + getValue("dept_name") + "\",";
		Map<String,String> costCenter = getCostCenter(getValue("cost_center"));
		if (costCenter != null) {
			csvData += "\"" + costCenter.get("id") + "\",";
			csvData += "\"" + costCenter.get("name") + "\",";
			csvData += "\"" + costCenter.get("accountId") + "\",";
			csvData += "\"" + costCenter.get("organizationId") + "\",";
			csvData += "\"" + costCenter.get("managerId") + "\",";
			csvData += "\"" + costCenter.get("manager") + "\"";
		} else {
			csvData += "\"\",\"\",\"\",\"\",\"\",\"\"";
		}
		csvData += LF;
		writeCt++;
	}

	private String getValue(String columnLabel) {
		String value = "";

		try {
			if (StringUtils.isNotEmpty(rs.getString(columnLabel))) {
				value = rs.getString(columnLabel).replace("\"", "\\\"");
			}
		} catch (SQLException e) {
			// no one cares
		}

		return value;
	}

	private Map<String,String> getCostCenter(String id) {
		Map<String,String> costCenter = null;

		String[] parts = id.split("-");
		if (parts.length == 3) {
			String owner = parts[1];
			id = parts[2];
			// ignore invalid cost centers
			if (validOwners.contains(owner) && id.length() == 4) {
				// if we already have it, use it
				if (cc.containsKey(id)) {
					costCenter = cc.get(id);
				} else {
					// if we haven't already tried and failed to get it, go get it
					if (!noCc.containsKey(id)) {
						costCenter = fetchCostCenter(id);
					}
				}
			}
		}

		return costCenter;
	}

	private Map<String,String> fetchCostCenter(String id) {
		Map<String,String> costCenter = null;

		String url = ccServiceURL + id;
		if (log.isDebugEnabled()) {
			log.debug("Opening URL " + url);
		}
		try {
			HttpGet get = new HttpGet(url);
			HttpResponse response = client.execute(get);
			int responseCode = response.getStatusLine().getStatusCode();
			String xml = EntityUtils.toString(response.getEntity());
			if (responseCode == 200) {
				costCenter = new HashMap<String,String>();
				costCenter.put("id", id);
				if (log.isDebugEnabled()) {
					log.debug("XML returned:\n" + xml);
				}
				Document document = db.parse(new InputSource(new StringReader(xml)));
				costCenter.put("name", getValueByTagName(document, "name"));
				costCenter.put("accountId", getValueByTagName(document, "account"));
				costCenter.put("organizationId", getValueByTagName(document, "organizationId"));
				costCenter.put("manager", getValueByTagName(document, "manager"));
				if (StringUtils.isNotEmpty(costCenter.get("manager"))) {
					costCenter.put("manager", costCenter.get("manager").trim());
					costCenter.put("managerId", document.getElementsByTagName("manager").item(0).getAttributes().getNamedItem("id").getNodeValue());
				} else {
					costCenter.put("manager", "");
					costCenter.put("managerId", "");
				}
				cc.put(id, costCenter);
			} else {
				if (responseCode == 404) {
					log.info("Record not found for cost center " + id);
					noCc.put(id, id);
				} else {
					log.error("Invalid response from URL " + url + ": " + responseCode);
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred processing URL " + url + ": " + e, e);
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning cost center: " + costCenter);
		}

		return costCenter;
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

	private void sendEmail(InternetAddress to) throws Exception {
		Message message = new MimeMessage(session);
		message.setFrom(fromAddress);
		message.addRecipient(Message.RecipientType.TO, to);
		message.setSubject(mailSubject);

		// Create the message part 
		BodyPart messageBodyPart = new MimeBodyPart();

		// Fill the message
		messageBodyPart.setText("See attached ...");

		Multipart multipart = new MimeMultipart();
		multipart.addBodyPart(messageBodyPart);

		// Part two is attachment
		messageBodyPart = new MimeBodyPart();
		messageBodyPart.setFileName(csvFileName);
		messageBodyPart.setContent(csvData, "text/comma-separated-values");
		multipart.addBodyPart(messageBodyPart);

		// Put parts in message
		message.setContent(multipart);

		// Send the message
		Transport.send(message);
		emailCt++;
	}

	private void send2ndEmail(InternetAddress to) throws Exception {
		Message message = new MimeMessage(session);
		message.setFrom(fromAddress);
		message.addRecipient(Message.RecipientType.TO, to);
		message.setSubject(mail2Subject);

		// Create the message body 
		String body = "The following Cost Centers were not found while processing the latest FD&C data:\n\n";
		Iterator<String> i = noCc.keySet().iterator();
		while (i.hasNext()) {
			body += i.next() + "\n";
		}

		// Put body in message
		message.setText(body);

		// Send the message
		Transport.send(message);
		email2Ct++;
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * @param mailServer the mailServer to set
	 */
	public void setMailServer(String mailServer) {
		this.mailServer = mailServer;
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
	 * @param mail2To the mail2To to set
	 */
	public void setMail2To(String mail2To) {
		this.mail2To = mail2To;
	}

	/**
	 * @param mailSubject the mailSubject to set
	 */
	public void setMailSubject(String mailSubject) {
		this.mailSubject = mailSubject;
	}

	/**
	 * @param mail2Subject the mail2Subject to set
	 */
	public void setMail2Subject(String mail2Subject) {
		this.mail2Subject = mail2Subject;
	}

	/**
	 * @param ccServiceURL the ccServiceURL to set
	 */
	public void setCcServiceURL(String ccServiceURL) {
		this.ccServiceURL = ccServiceURL;
	}
}
