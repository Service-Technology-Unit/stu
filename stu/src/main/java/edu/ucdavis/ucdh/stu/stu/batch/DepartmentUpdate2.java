package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
 * <p>Updates the Service Manager department list from the PeopleSoft department list.</p>
 */
public class DepartmentUpdate2 implements SpringBatchJob {
	private static final String DEPT_UNKNOWN = "Department Unknown";
	private final Log log = LogFactory.getLog(getClass().getName());
	private Connection psConn = null;
	private Statement psStmt = null;
	private ResultSet psRs = null;
	private Connection smConn = null;
	private Statement smStmt = null;
	private ResultSet smRs = null;
	private String psDriver = null;
	private String psURL = null;
	private String psUser = null;
	private String psPassword = null;
	private String smDriver = null;
	private String smURL = null;
	private String smUser = null;
	private String smPassword = null;
	private String up2dateService = null;
	private String publisherId = null;
	private Department peopleSoftRecord = null;
	private Department serviceManagerRecord = null;
	private List<Department> activatePile = new ArrayList<Department>();
	private List<Department> reactivatePile = new ArrayList<Department>();
	private List<Department> updatePile = new ArrayList<Department>();
	private List<Department> deactivatePile = new ArrayList<Department>();
	private List<NameChange> nameChangePile = new ArrayList<NameChange>();
	private HttpClient client = null;
	private int jobId = 9999;
	private int peopleSoftRecordsRead = 0;
	private int serviceManagerRecordsRead = 0;
	private int newDepartmentsInserted = 0;
	private int existingDepartmentsReactivated = 0;
	private int existingDepartmentsUpdated = 0;
	private int existingDepartmentsDeactivated = 0;
	private int existingContactsUpdated = 0;
	private int contactsUpdatedToUnknown = 0;
	private int failedInserts = 0;
	private int failedUpdates = 0;
	private int failedContactUpdates = 0;
	private int matchesFound = 0;
	private int dataMatchesFound = 0;
	private int loginIdsFound = 0;
	private int loginIdsNotFound = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		departmentUpdateBegin();
		while (peopleSoftRecord != null || serviceManagerRecord != null) {
			if (peopleSoftRecord == null) {
				// no more peopleSoft records -- everything else is a delete
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord is null; serviceManagerRecord.getId(): " + serviceManagerRecord.getId());
				}
				processDelete();
			} else if (serviceManagerRecord == null) {
				// no more serviceManager records -- everything else is an add
				if (log.isDebugEnabled()) {
					log.debug("serviceManagerRecord is null; peopleSoftRecord.getId(): " + peopleSoftRecord.getId());
				}
				processAdd();
			} else if (peopleSoftRecord.getId().equals(serviceManagerRecord.getId())) {
				// no change from serviceManager -- this is a match
				if (log.isDebugEnabled()) {
					log.debug("serviceManagerRecord == peopleSoftRecord; id: " + peopleSoftRecord.getId());
				}
				processMatch();
			} else if (peopleSoftRecord.getId().compareTo(serviceManagerRecord.getId()) < 0) {
				// peopleSoft < serviceManager -- this is an add
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord < serviceManagerRecord; peopleSoftRecord.getId(): " + peopleSoftRecord.getId() + "; serviceManagerRecord.getId(): " + serviceManagerRecord.getId());
				}
				processAdd();
			} else {
				// serviceManager < peopleSoft -- this is a delete
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord > serviceManagerRecord; peopleSoftRecord.getId(): " + peopleSoftRecord.getId() + "; serviceManagerRecord.getId(): " + serviceManagerRecord.getId());
				}
				processDelete();
			}
		}
		return departmentUpdateEnd();
	}

	private void departmentUpdateBegin() throws Exception {
		log.info("DepartmentUpdate starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify psDriver
		if (StringUtils.isEmpty(psDriver)) {
			throw new IllegalArgumentException("Required property \"psDriver\" missing or invalid.");
		} else {
			log.info("psDriver = " + psDriver);
		}
		// verify psUser
		if (StringUtils.isEmpty(psUser)) {
			throw new IllegalArgumentException("Required property \"psUser\" missing or invalid.");
		} else {
			log.info("psUser = " + psUser);
		}
		// verify psPassword
		if (StringUtils.isEmpty(psPassword)) {
			throw new IllegalArgumentException("Required property \"psPassword\" missing or invalid.");
		} else {
			log.info("psPassword = ********");
		}
		// verify psURL
		if (StringUtils.isEmpty(psURL)) {
			throw new IllegalArgumentException("Required property \"psURL\" missing or invalid.");
		} else {
			log.info("psURL = " + psURL);
		}
		// verify smDriver
		if (StringUtils.isEmpty(smDriver)) {
			throw new IllegalArgumentException("Required property \"smDriver\" missing or invalid.");
		} else {
			log.info("smDriver = " + smDriver);
		}
		// verify smUser
		if (StringUtils.isEmpty(smUser)) {
			throw new IllegalArgumentException("Required property \"smUser\" missing or invalid.");
		} else {
			log.info("smUser = " + smUser);
		}
		// verify smPassword
		if (StringUtils.isEmpty(smPassword)) {
			throw new IllegalArgumentException("Required property \"smPassword\" missing or invalid.");
		} else {
			log.info("smPassword = ********");
		}
		// verify smURL
		if (StringUtils.isEmpty(smURL)) {
			throw new IllegalArgumentException("Required property \"smURL\" missing or invalid.");
		} else {
			log.info("smURL = " + smURL);
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// connect to PeopleSoft database
		Class.forName(psDriver);
		psConn = DriverManager.getConnection(psURL, psUser, psPassword);
		psStmt = psConn.createStatement();
		psRs = psStmt.executeQuery("SELECT DEPTID, GL_EXPENSE, DESCR, NATIONAL_ID FROM PS_UC_DEP_MGR_VW WHERE GL_EXPENSE > '' ORDER BY DEPTID");

		// connect to ServiceManager database
		Class.forName(smDriver);
		smConn = DriverManager.getConnection(smURL, smUser, smPassword);
		smStmt = smConn.createStatement();
		smRs = smStmt.executeQuery("SELECT d.DEPT_ID, d.DEPT_NAME, c.USER_ID, d.UCD_DEPT_CODE, d.UCD_ACTIVE FROM DEPTM1 d LEFT OUTER JOIN CONTCTSM1 c ON c.CONTACT_NAME=d.UCD_DEPT_MGR ORDER BY d.DEPT_ID");

    	// get first set of records
    	getNextPeopleSoftRecord();
    	getNextServiceManagerRecord();
	}

    private List<BatchJobServiceStatistic> departmentUpdateEnd() throws Exception {
		// close PeopleSoft connection
		psRs.close();
		psStmt.close();
		psConn.close();

		// close ServiceManager query
		smRs.close();
		smStmt.close();

		// establish HTTP Client
		client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();

		// insert new departments
		if (activatePile.size() > 0) {
			log.info("Adding " + activatePile.size() + " new department(s).");
			log.info(" ");
			for (int i=0; i<activatePile.size(); i++) {
				Department thisDepartment = activatePile.get(i);
				List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
				urlParameters.add(new BasicNameValuePair("_pid", publisherId));
				urlParameters.add(new BasicNameValuePair("_jid", jobId + ""));
				urlParameters.add(new BasicNameValuePair("_action", "add"));
				urlParameters.add(new BasicNameValuePair("deptId", thisDepartment.getId()));
				urlParameters.add(new BasicNameValuePair("name", thisDepartment.getName()));
				urlParameters.add(new BasicNameValuePair("managerPpsid", thisDepartment.getManager()));
				urlParameters.add(new BasicNameValuePair("ppsDeptId", thisDepartment.getAltId()));
				postToService(urlParameters);
				newDepartmentsInserted++;
			}
		}

		// reactivate existing departments
		if (reactivatePile.size() > 0) {
			log.info("Reactivating " + reactivatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<reactivatePile.size(); i++) {
				Department thisDepartment = reactivatePile.get(i);
				List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
				urlParameters.add(new BasicNameValuePair("_pid", publisherId));
				urlParameters.add(new BasicNameValuePair("_jid", jobId + ""));
				urlParameters.add(new BasicNameValuePair("_action", "change"));
				urlParameters.add(new BasicNameValuePair("deptId", thisDepartment.getId()));
				urlParameters.add(new BasicNameValuePair("name", thisDepartment.getName()));
				urlParameters.add(new BasicNameValuePair("managerPpsid", thisDepartment.getManager()));
				urlParameters.add(new BasicNameValuePair("ppsDeptId", thisDepartment.getAltId()));
				postToService(urlParameters);
				existingDepartmentsReactivated++;
			}
		}

		// update the name of existing departments
		if (updatePile.size() > 0) {
			log.info("Updating " + updatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<updatePile.size(); i++) {
				Department thisDepartment = updatePile.get(i);
				List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
				urlParameters.add(new BasicNameValuePair("_pid", publisherId));
				urlParameters.add(new BasicNameValuePair("_jid", jobId + ""));
				urlParameters.add(new BasicNameValuePair("_action", "change"));
				urlParameters.add(new BasicNameValuePair("deptId", thisDepartment.getId()));
				urlParameters.add(new BasicNameValuePair("name", thisDepartment.getName()));
				urlParameters.add(new BasicNameValuePair("managerPpsid", thisDepartment.getManager()));
				urlParameters.add(new BasicNameValuePair("ppsDeptId", thisDepartment.getAltId()));
				postToService(urlParameters);
				existingDepartmentsUpdated++;
			}
		}

		// deactivate existing departments
		if (deactivatePile.size() > 0) {
			log.info("Deactivating " + deactivatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<deactivatePile.size(); i++) {
				Department thisDepartment = deactivatePile.get(i);
				List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
				urlParameters.add(new BasicNameValuePair("_pid", publisherId));
				urlParameters.add(new BasicNameValuePair("_jid", jobId + ""));
				urlParameters.add(new BasicNameValuePair("_action", "delete"));
				urlParameters.add(new BasicNameValuePair("deptId", thisDepartment.getId()));
				urlParameters.add(new BasicNameValuePair("name", thisDepartment.getName()));
				urlParameters.add(new BasicNameValuePair("managerPpsid", thisDepartment.getManager()));
				urlParameters.add(new BasicNameValuePair("ppsDeptId", thisDepartment.getAltId()));
				postToService(urlParameters);
				existingDepartmentsDeactivated++;
			}
		}

		// update contacts with new department names
		if (nameChangePile.size() > 0) {
			log.info("Updating contacts for " + nameChangePile.size() + " department name change(s).");
			log.info(" ");
			for (int i=0; i<nameChangePile.size(); i++) {
				NameChange nameChange = nameChangePile.get(i);
				PreparedStatement ps = null;
				try {
					ps = smConn.prepareStatement("update dbo.CONTCTSM1 set DEPT_NAME=? where DEPT_NAME=?");
					ps.setString(1, nameChange.getNewName());
					ps.setString(2, nameChange.getOldName());
					int updateCt = ps.executeUpdate();
					if (updateCt > 0) {
						if (DEPT_UNKNOWN.equals(nameChange.getNewName())) {
							contactsUpdatedToUnknown += updateCt;
						} else {
							existingContactsUpdated += updateCt;
						}
					} else {
						failedContactUpdates++;
						log.error("Unable to update contacts for old department " + nameChange.getOldName());
					}
				} catch (Exception e) {
					failedContactUpdates++;
					log.error("Exception encountered while attempting to update contacts for old department " + nameChange.getOldName() + "; " + e.getMessage(), e);
				} finally {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		}

		// close ServiceManager connection
		smConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (peopleSoftRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("PeopleSoft records read", BatchJobService.FORMAT_INTEGER, new BigInteger(peopleSoftRecordsRead + "")));
		}
		if (loginIdsFound > 0) {
			stats.add(new BatchJobServiceStatistic("A/D login IDs found", BatchJobService.FORMAT_INTEGER, new BigInteger(loginIdsFound + "")));
		}
		if (loginIdsNotFound > 0) {
			stats.add(new BatchJobServiceStatistic("A/D login IDs not found", BatchJobService.FORMAT_INTEGER, new BigInteger(loginIdsNotFound + "")));
		}
		if (serviceManagerRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("ServiceManager records read", BatchJobService.FORMAT_INTEGER, new BigInteger(serviceManagerRecordsRead + "")));
		}
		if (matchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Departments found in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(matchesFound + "")));
		}
		if (dataMatchesFound > 0) {
			stats.add(new BatchJobServiceStatistic("Departments the same in both systems", BatchJobService.FORMAT_INTEGER, new BigInteger(dataMatchesFound + "")));
		}
		if (newDepartmentsInserted > 0) {
			stats.add(new BatchJobServiceStatistic("New departments added", BatchJobService.FORMAT_INTEGER, new BigInteger(newDepartmentsInserted + "")));
		}
		if (existingDepartmentsReactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing departments reactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingDepartmentsReactivated + "")));
		}
		if (existingDepartmentsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing departments updated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingDepartmentsUpdated + "")));
		}
		if (existingDepartmentsDeactivated > 0) {
			stats.add(new BatchJobServiceStatistic("Existing departments deactivated", BatchJobService.FORMAT_INTEGER, new BigInteger(existingDepartmentsDeactivated + "")));
		}
		if (existingContactsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("Contacts updated with new department name", BatchJobService.FORMAT_INTEGER, new BigInteger(existingContactsUpdated + "")));
		}
		if (contactsUpdatedToUnknown > 0) {
			stats.add(new BatchJobServiceStatistic("Contacts updated to unknown department", BatchJobService.FORMAT_INTEGER, new BigInteger(contactsUpdatedToUnknown + "")));
		}
		if (failedInserts > 0) {
			stats.add(new BatchJobServiceStatistic("Failed insert attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedInserts + "")));
		}
		if (failedUpdates > 0) {
			stats.add(new BatchJobServiceStatistic("Failed update attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedUpdates + "")));
		}
		if (failedContactUpdates > 0) {
			stats.add(new BatchJobServiceStatistic("Failed contact update attempts", BatchJobService.FORMAT_INTEGER, new BigInteger(failedContactUpdates + "")));
		}

		// end job
		log.info("DepartmentUpdate complete.");

		return stats;
	}

	/**
	 * <p>Processes a match.</p>
	 */
	private void processMatch() throws Exception {
		matchesFound++;
		if (serviceManagerRecord.isActive()) {
			if (serviceManagerRecord.getName().equals(peopleSoftRecord.getName())) {
				if (serviceManagerRecord.getManager().equals(peopleSoftRecord.getManager())) {
					if (serviceManagerRecord.getAltId().equals(peopleSoftRecord.getAltId())) {
						dataMatchesFound++;
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Unmatched PPS Dept ID for Dept " + serviceManagerRecord.getId() + ": " + serviceManagerRecord.getAltId() + "; " + peopleSoftRecord.getAltId());
						}
						updatePile.add(peopleSoftRecord);
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Unmatched Manager ID for Dept " + serviceManagerRecord.getId() + ": " + serviceManagerRecord.getManager() + "; " + peopleSoftRecord.getManager());
					}
					updatePile.add(peopleSoftRecord);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Unmatched Name for Dept " + serviceManagerRecord.getId() + ": " + serviceManagerRecord.getName() + "; " + peopleSoftRecord.getName());
				}
				updatePile.add(peopleSoftRecord);
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), peopleSoftRecord.getName()));
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Reactivating inactive Dept " + serviceManagerRecord.getId());
			}
			reactivatePile.add(peopleSoftRecord);
			if (!serviceManagerRecord.getName().equals(peopleSoftRecord.getName())) {
				if (log.isDebugEnabled()) {
					log.debug("Unmatched Name for Dept " + serviceManagerRecord.getId() + ": " + serviceManagerRecord.getName() + "; " + peopleSoftRecord.getName());
				}
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), peopleSoftRecord.getName()));
			}
		}
		getNextPeopleSoftRecord();
		getNextServiceManagerRecord();
	}

	/**
	 * <p>Processes an add.</p>
	 */
	private void processAdd() throws Exception {
		activatePile.add(peopleSoftRecord);
		getNextPeopleSoftRecord();
	}

	/**
	 * <p>Processes a delete.</p>
	 */
	private void processDelete() throws Exception {
		if (!"UNKNOWN".equals(serviceManagerRecord.getId())) {
			if (serviceManagerRecord.isActive()) {
				deactivatePile.add(serviceManagerRecord);
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), DEPT_UNKNOWN));
			}
		}
		getNextServiceManagerRecord();
	}

	/**
	 * <p>Get next peopleSoft record.</p>
	 */
	private void getNextPeopleSoftRecord() throws Exception {
		if (psRs.next()) {
		   	peopleSoftRecord = new Department(psRs.getString("DEPTID"), psRs.getString("DESCR"), psRs.getString("NATIONAL_ID"), psRs.getString("GL_EXPENSE"), true);
			peopleSoftRecordsRead++;
		} else {
			peopleSoftRecord = null;
		}
	}

	/**
	 * <p>Get next serviceManager record.</p>
	 */
	private void getNextServiceManagerRecord() throws Exception {
		if (smRs.next()) {
			serviceManagerRecord = new Department(smRs.getString("DEPT_ID"), smRs.getString("DEPT_NAME"), smRs.getString("USER_ID"), smRs.getString("UCD_DEPT_CODE"), "t".equalsIgnoreCase(smRs.getString("UCD_ACTIVE")));
			serviceManagerRecordsRead++;
		} else {
			serviceManagerRecord = null;
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

	private class Department {
		private String id = null;
		private String name = null;
		private String manager = null;
		private String altId = null;
		private boolean active = true;

		public Department(String id, String name, String manager, String altId, boolean active) {
			if (id != null) {
				this.id = id.trim();
			} else {
				this.id = "";
			}
			if (name != null) {
				this.name = name.trim();
			} else {
				this.name = "";
			}
			if (manager != null) {
				this.manager = manager.trim();
			} else {
				this.manager = "";
			}
			if (altId != null) {
				this.altId = altId.trim();
			} else {
				this.altId = "";
			}
			this.active = active;
		}

		/**
		 * @return the id
		 */
		public String getId() {
			return id;
		}

		/**
		 * @return the name
		 */
		public String getName() {
			return name;
		}

		/**
		 * @return the manager
		 */
		public String getManager() {
			return manager;
		}

		/**
		 * @return the altId
		 */
		public String getAltId() {
			return altId;
		}

		/**
		 * @return the active
		 */
		public boolean isActive() {
			return active;
		}
	}

	private class NameChange {
		private String oldName = null;
		private String newName = null;

		public NameChange(String oldName, String newName) {
			this.oldName = oldName;
			this.newName = newName;
		}

		/**
		 * @return the oldName
		 */
		public String getOldName() {
			return oldName;
		}

		/**
		 * @return the newName
		 */
		public String getNewName() {
			return newName;
		}
	}

	/**
	 * @return the psDriver
	 */
	public String getPsDriver() {
		return psDriver;
	}

	/**
	 * @param psDriver the psDriver to set
	 */
	public void setPsDriver(String psDriver) {
		this.psDriver = psDriver;
	}

	/**
	 * @return the psURL
	 */
	public String getPsURL() {
		return psURL;
	}

	/**
	 * @param psURL the psURL to set
	 */
	public void setPsURL(String psURL) {
		this.psURL = psURL;
	}

	/**
	 * @return the psUser
	 */
	public String getPsUser() {
		return psUser;
	}

	/**
	 * @param psUser the psUser to set
	 */
	public void setPsUser(String psUser) {
		this.psUser = psUser;
	}

	/**
	 * @return the psPassword
	 */
	public String getPsPassword() {
		return psPassword;
	}

	/**
	 * @param psPassword the psPassword to set
	 */
	public void setPsPassword(String psPassword) {
		this.psPassword = psPassword;
	}

	/**
	 * @return the smDriver
	 */
	public String getSmDriver() {
		return smDriver;
	}

	/**
	 * @param smDriver the smDriver to set
	 */
	public void setSmDriver(String smDriver) {
		this.smDriver = smDriver;
	}

	/**
	 * @return the smURL
	 */
	public String getSmURL() {
		return smURL;
	}

	/**
	 * @param smURL the smURL to set
	 */
	public void setSmURL(String smURL) {
		this.smURL = smURL;
	}

	/**
	 * @return the smUser
	 */
	public String getSmUser() {
		return smUser;
	}

	/**
	 * @param smUser the smUser to set
	 */
	public void setSmUser(String smUser) {
		this.smUser = smUser;
	}

	/**
	 * @return the smPassword
	 */
	public String getSmPassword() {
		return smPassword;
	}

	/**
	 * @param smPassword the smPassword to set
	 */
	public void setSmPassword(String smPassword) {
		this.smPassword = smPassword;
	}

	/**
	 * @return the up2dateService
	 */
	public String getUp2dateService() {
		return up2dateService;
	}

	/**
	 * @param up2dateService the up2dateService to set
	 */
	public void setUp2dateService(String up2dateService) {
		this.up2dateService = up2dateService;
	}

	/**
	 * @return the publisherId
	 */
	public String getPublisherId() {
		return publisherId;
	}

	/**
	 * @param publisherId the publisherId to set
	 */
	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}
}
