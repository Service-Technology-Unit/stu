package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.net.InetAddress;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

/**
 * <p>Updates the Master department list from the PeopleSoft department list.</p>
 */
public class MasterDepartmentUpdate implements SpringBatchJob {
	private static final String TODAY = DateFormat.getDateInstance(DateFormat.SHORT).format(new Date());
	private static final String PS_SQL = "SELECT DEPTID, GL_EXPENSE, DESCR, NATIONAL_ID FROM PS_UC_DEP_MGR_VW WHERE GL_EXPENSE>'' AND LENGTH(DEPTID)>6 ORDER BY DEPTID";
	private static final String PR_SQL = "SELECT ID, ALTERNATE_ID, COST_CENTER_ID, NAME, MANAGER, IS_ACTIVE FROM DEPARTMENT ORDER BY ID";
	private static final String INSERT_SQL = "INSERT INTO DEPARTMENT (ID, IS_ACTIVE, ALTERNATE_ID, COST_CENTER_ID, NAME, DESCRIPTION, MANAGER, START_DATE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES (?, 'Y', ?, ?, ?, ?, ?, ?, 0, ?, getdate(), ?)";
	private static final String UPDATE_SQL = "UPDATE DEPARTMENT SET IS_ACTIVE=?, ALTERNATE_ID=?, COST_CENTER_ID=?, NAME=?, DESCRIPTION=?, MANAGER=?, END_DATE=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER=?, SYSMODTIME=getdate(), SYSMODADDR=? WHERE ID=?";
	private static final String UPDATE_HISTORY_SQL = "INSERT INTO DEPARTMENT_HISTORY (DEPARTMENT_ID, COLUMN_NAME, OLD_VALUE, NEW_VALUE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES(?, ?, ?, ?, getdate(), ?, 0, ?)";
	private static final String IAMID_SQL = "SELECT PERSON_ID FROM PERSON_ID WHERE ID_TYPE='PPS' AND ID_VALUE=? ORDER BY IS_PRIMARY DESC, IS_ACTIVE DESC";
	private static final String USER_ID = System.getProperty("user.name");
	private final Log log = LogFactory.getLog(getClass());
	private String localAddress = null;
	private Connection psConn = null;
	private Statement psStmt = null;
	private ResultSet psRs = null;
	private Connection prConn = null;
	private Statement prStmt = null;
	private ResultSet prRs = null;
	private DataSource prDataSource = null;
	private DataSource psDataSource = null;
	private Up2DateService up2dateService = null;
	private String costCenterService = null;
	private String publisherId = null;
	private List<Department> peopleSoftRecord = new ArrayList<Department>();
	private List<Department> masterRecord = new ArrayList<Department>();
	private HttpClient client = null;
	private int psIndex = 0;
	private int prIndex = 0;
	private int peopleSoftRecordsRead = 0;
	private int masterRecordsRead = 0;
	private int newDepartmentsInserted = 0;
	private int existingDepartmentsReactivated = 0;
	private int existingDepartmentsUpdated = 0;
	private int existingDepartmentsDeactivated = 0;
	private int failedInserts = 0;
	private int failedUpdates = 0;
	private int failedHistoryInserts = 0;
	private int matchesFound = 0;
	private int dataMatchesFound = 0;
	private int up2dateServiceCalls = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		departmentUpdateBegin();
		while (psIndex < peopleSoftRecord.size() || prIndex < masterRecord.size()) {
			if (psIndex >= peopleSoftRecord.size()) {
				// no more peopleSoft records -- everything else is a delete
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord is null; masterRecord.getId(): " + masterRecord.get(prIndex).getId());
				}
				processDelete();
			} else if (prIndex >= masterRecord.size()) {
				// no more master records -- everything else is an add
				if (log.isDebugEnabled()) {
					log.debug("masterRecord is null; peopleSoftRecord.getId(): " + peopleSoftRecord.get(psIndex).getId());
				}
				processAdd();
			} else if (peopleSoftRecord.get(psIndex).getId().equals(masterRecord.get(prIndex).getId())) {
				// no change from master -- this is a match
				if (log.isDebugEnabled()) {
					log.debug("masterRecord == peopleSoftRecord; id: " + peopleSoftRecord.get(psIndex).getId());
				}
				processMatch();
			} else if (peopleSoftRecord.get(psIndex).getId().compareTo(masterRecord.get(prIndex).getId()) < 0) {
				// peopleSoft < master -- this is an add
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord < masterRecord; peopleSoftRecord.getId(): " + peopleSoftRecord.get(psIndex).getId() + "; masterRecord.getId(): " + masterRecord.get(prIndex).getId());
				}
				processAdd();
			} else {
				// master < peopleSoft -- this is a delete
				if (log.isDebugEnabled()) {
					log.debug("peopleSoftRecord > masterRecord; peopleSoftRecord.getId(): " + peopleSoftRecord.get(psIndex).getId() + "; masterRecord.getId(): " + masterRecord.get(prIndex).getId());
				}
				processDelete();
			}
		}
		return departmentUpdateEnd();
	}

	private void departmentUpdateBegin() throws Exception {
		log.info("MasterDepartmentUpdate starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify prDataSource
		if (prDataSource == null) {
			throw new IllegalArgumentException("Required property \"prDataSource\" missing or invalid.");
		} else {
			try {
				prConn = prDataSource.getConnection();
				log.info("Connection established to prDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to prDataSource: " + e, e);
			}
		}
		// verify psDataSource
		if (psDataSource == null) {
			throw new IllegalArgumentException("Required property \"psDataSource\" missing or invalid.");
		} else {
			try {
				psConn = psDataSource.getConnection();
				log.info("Connection established to psDataSource");
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to connect to psDataSource: " + e, e);
			}
		}
		// verify up2dateService
		if (up2dateService == null) {
			throw new IllegalArgumentException("Required property \"up2dateService\" missing or invalid.");
		}
		log.info("Up2Date service validated.");
		// verify costCenterService
		if (StringUtils.isEmpty(costCenterService)) {
			throw new IllegalArgumentException("Required property \"costCenterService\" missing or invalid.");
		}
		log.info("Using costCenterService " + costCenterService);
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

		// establish HTTP Client
		client = createHttpClient();

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// connect to PeopleSoft database
		psConn = psDataSource.getConnection();
		psStmt = psConn.createStatement();
		psRs = psStmt.executeQuery(PS_SQL);
		while (psRs.next()) {
			peopleSoftRecordsRead++;
			peopleSoftRecord.add(new Department(psRs.getString("DEPTID"), psRs.getString("GL_EXPENSE"), getCostCenterId(psRs.getString("DEPTID")), psRs.getString("DESCR"), getIAMIDfromPPSID(psRs.getString("NATIONAL_ID")), true));
		}
		psRs.close();
		psStmt.close();
		psConn.close();

		// connect to MasterData database
		prConn = prDataSource.getConnection();
		prStmt = prConn.createStatement();
		prRs = prStmt.executeQuery(PR_SQL);
		while (prRs.next()) {
			masterRecordsRead++;
			masterRecord.add(new Department(prRs.getString("ID"), prRs.getString("ALTERNATE_ID"), prRs.getString("COST_CENTER_ID"), prRs.getString("NAME"), prRs.getString("MANAGER"), "Y".equalsIgnoreCase(prRs.getString("IS_ACTIVE"))));
		}
		prRs.close();
		prStmt.close();
	}

    private List<BatchJobServiceStatistic> departmentUpdateEnd() throws Exception {
		// close MasterData connection
		prConn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (peopleSoftRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("PeopleSoft records read", BatchJobService.FORMAT_INTEGER, new BigInteger(peopleSoftRecordsRead + "")));
		}
		if (masterRecordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("MasterData records read", BatchJobService.FORMAT_INTEGER, new BigInteger(masterRecordsRead + "")));
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
		log.info("MasterDepartmentUpdate complete.");

		return stats;
	}

	/**
	 * <p>Processes a match.</p>
	 */
	private void processMatch() throws Exception {
		matchesFound++;
		if (masterRecord.get(prIndex).equals(peopleSoftRecord.get(psIndex))) {
			dataMatchesFound++;
		} else {
			updateDepartment(masterRecord.get(prIndex), peopleSoftRecord.get(psIndex), false);
		}
		psIndex++;
		prIndex++;
	}

	/**
	 * <p>Processes an add.</p>
	 */
	private void processAdd() throws Exception {
		insertDepartment(peopleSoftRecord.get(psIndex));
		psIndex++;
	}

	/**
	 * <p>Processes a delete.</p>
	 */
	private void processDelete() throws Exception {
		if (masterRecord.get(prIndex).isActive()) {
			updateDepartment(null, masterRecord.get(prIndex), true);
		}
		prIndex++;
	}

	private void insertDepartment(Department thisDepartment) {
		if (log.isDebugEnabled()) {
			log.debug("Inserting new Department: " + thisDepartment.getId());
			log.debug("Using the following SQL: " + INSERT_SQL);
		}

		// insert into database
		PreparedStatement ps = null;
		try {
			ps = prConn.prepareStatement(INSERT_SQL);
			ps.setString(1, thisDepartment.getId());
			ps.setString(2, thisDepartment.getAltId());
			ps.setString(3, thisDepartment.getCcId());
			ps.setString(4, thisDepartment.getName());
			ps.setString(5, thisDepartment.getName());
			ps.setString(6, thisDepartment.getManager());
			ps.setString(7, TODAY);
			ps.setString(8, USER_ID);
			ps.setString(9, localAddress);
			if (ps.executeUpdate() > 0) {
				newDepartmentsInserted++;
				// publish
				Properties properties = new Properties();
				properties.setProperty("id", thisDepartment.getId());
				properties.setProperty("alternateId", thisDepartment.getAltId());
				properties.setProperty("costCenterId", thisDepartment.getCcId());
				properties.setProperty("name", thisDepartment.getName());
				properties.setProperty("description", thisDepartment.getName());
				properties.setProperty("manager", thisDepartment.getManager());
				properties.setProperty("isActive", thisDepartment.isActive()?"Y":"N");
				properties.setProperty("startDate", TODAY);
				properties.setProperty("endDate", "");
				up2dateService.post(new Update(publisherId, "add", properties));
				up2dateServiceCalls++;
			} else {
				failedInserts++;
				log.error("Unable to insert record for department " + thisDepartment.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to insert record for department " + thisDepartment.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	private void updateDepartment(Department oldDepartment,  Department newDepartment, boolean deactivate) {
		if (log.isDebugEnabled()) {
			if (deactivate) {
				log.debug("Deactivating existing Department: " + newDepartment.getId());
			} else {
				log.debug("Updating existing Department: " + newDepartment.getId());
			}
			log.debug("Using the following SQL: " + UPDATE_SQL);
		}

		// insert into database
		PreparedStatement ps = null;
		try {
			ps = prConn.prepareStatement(UPDATE_SQL);
			if (deactivate) {
				ps.setString(1, "N");
				ps.setString(7, TODAY);
			} else {
				ps.setString(1, "Y");
				ps.setString(7, null);
			}
			ps.setString(2, newDepartment.getAltId());
			ps.setString(3, newDepartment.getCcId());
			ps.setString(4, newDepartment.getName());
			ps.setString(5, newDepartment.getName());
			ps.setString(6, newDepartment.getManager());
			ps.setString(8, USER_ID);
			ps.setString(9, localAddress);
			ps.setString(10, newDepartment.getId());
			if (ps.executeUpdate() > 0) {
				Properties properties = new Properties();
				String action = "";
				if (deactivate) {
					existingDepartmentsDeactivated++;
					action = "delete";
					properties.setProperty("isActive", "N");
					properties.setProperty("endDate", TODAY);
				} else {
					existingDepartmentsUpdated++;
					action = "change";
					properties.setProperty("isActive", "Y");
					properties.setProperty("endDate", "");
				}
				properties.setProperty("id", newDepartment.getId());
				properties.setProperty("alternateId", newDepartment.getAltId());
				properties.setProperty("costCenterId", newDepartment.getCcId());
				properties.setProperty("name", newDepartment.getName());
				properties.setProperty("description", newDepartment.getName());
				properties.setProperty("manager", newDepartment.getManager());
				up2dateService.post(new Update(publisherId, action, properties));
				up2dateServiceCalls++;
				updateDepartmentHistory(oldDepartment, newDepartment);
			} else {
				failedInserts++;
				log.error("Unable to update record for department " + newDepartment.getId());
			}
		} catch (Exception e) {
			failedInserts++;
			log.error("Exception encountered while attempting to update record for department " + newDepartment.getId() + "; " + e.getMessage(), e);
		} finally {
			try {
				ps.close();
			} catch (Exception e) {
				// no one cares!
			}
		}
	}

	/**
	 * <p>Updates the department history for any column updated.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newDepartment the new values for the department
	 * @param oldDepartment the old values for the department
	 */
	private void updateDepartmentHistory(Department oldDepartment,  Department newDepartment) {
		String id = newDepartment.getId();
		if (log.isDebugEnabled()) {
			log.debug("Updating department history for department " + id);
		}

		// find changed columns
		List<Map<String,String>> change = newDepartment.getChanges(oldDepartment);

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for department " + id);
			}
			PreparedStatement ps = null;
			try {
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + UPDATE_HISTORY_SQL);
				}
				ps = prConn.prepareStatement(UPDATE_HISTORY_SQL);
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
							log.debug("Department history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update department history for department " + id + "; " + e.getMessage(), e);
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
				log.debug("No changes detected for department " + id);
			}
		}
	}

	private String getCostCenterId(String id) {
		String costCenterId = "";
		
		String url = costCenterService + id.replace(" ", "");
		HttpGet get = new HttpGet(url);
		String resp = "";
		int rc = 0;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Accessing cost center using the following URL: " + url);
			}
			HttpResponse response = client.execute(get);
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
					}
					if (StringUtils.isNotEmpty(resp) && resp.indexOf("<field name=\"id\">") != -1) {
						costCenterId = resp.substring(resp.indexOf("<field name=\"id\">") + 17, resp.indexOf("<field name=\"id\">") + 21);
					}
				}
			} else {
				try {
					resp = EntityUtils.toString(response.getEntity());
				} catch (Exception e) {
					// no one cares
				}
				if (rc == 404) {
					if (log.isDebugEnabled()) {
						log.debug("Cost Center not found for department " + id);
					} else {
						log.error("Invalid response code (" + rc + ") encountered accessing URL " + url);
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + url, e);
		}

		return costCenterId;
	}

	/**
	 * <p>Returns the IAM ID associated with the provided PPS ID.</p>
	 *
	 * @param ppsId the PPS ID of the person
	 * @return the IAM ID associated with the provided PPS ID
	 */
	private String getIAMIDfromPPSID(String ppsId) {
		String iamId = null;

		if (StringUtils.isNotEmpty(ppsId)) {
			iamId = "PPS: " + ppsId;			if (log.isDebugEnabled()) {
				log.debug("Fetching IAM ID for ppsId " + ppsId);
			}
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				ps = prConn.prepareStatement(IAMID_SQL);
				ps.setString(1, ppsId);
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + IAMID_SQL);
				}
				rs = ps.executeQuery();
				if (rs.next()) {
					if (log.isDebugEnabled()) {
						log.debug("IAM ID found for ppsId " + ppsId);
					}
					iamId = rs.getString("PERSON_ID");
				} else {
					if (log.isDebugEnabled()) {
						log.debug("IAM ID not found for ppsId " + ppsId);
					}
				}
			} catch (SQLException e) {
				log.error("Exception encountered fetching IAM ID for ppsId " + ppsId + ": " + e.getMessage(), e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception e) {
						//
					}
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception e) {
						//
					}
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning IAM ID: " + iamId);
		}

		return iamId;
	}

	private class Department {
		private String id = null;
		private String name = null;
		private String manager = null;
		private String altId = null;
		private String ccId = null;
		private boolean active = true;

		public Department(String id, String altId, String ccId, String name, String manager, boolean active) {
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
			if (ccId != null) {
				this.ccId = ccId.trim();
			} else {
				this.altId = "";
			}
			this.active = active;
		}

		/**
		 * @return true if this Department has the same values as the passed department
		 */
		public boolean equals(Department department) {
			return isEqual(department.id, this.id) && isEqual(department.name, this.name) && isEqual(department.manager, this.manager) && isEqual(department.altId, this.altId) && isEqual(department.ccId, this.ccId) && isEqual(department.active + "", this.active + "");
		}

		/**
		 * @return a list of changes between this department and the department passed
		 */
		public List<Map<String,String>> getChanges(Department department) {
			List<Map<String,String>> change = new ArrayList<Map<String,String>>();

			if (department == null) {
				Map<String,String> thisChange = new HashMap<String,String>();
				thisChange.put("COLUMN_NAME", "IS_ACTIVE");
				thisChange.put("OLD_VALUE", "Y");
				thisChange.put("NEW_VALUE", "N");
				change.add(thisChange);
			} else {
				if (!isEqual(this.name, department.name)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "NAME");
					thisChange.put("OLD_VALUE", department.name);
					thisChange.put("NEW_VALUE", this.name);
					change.add(thisChange);
				}
				if (!isEqual(this.manager, department.manager)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "MANAGER");
					thisChange.put("OLD_VALUE", department.manager);
					thisChange.put("NEW_VALUE", this.manager);
					change.add(thisChange);
				}
				if (!isEqual(this.altId, department.altId)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "ALTERNATE_ID");
					thisChange.put("OLD_VALUE", department.altId);
					thisChange.put("NEW_VALUE", this.altId);
					change.add(thisChange);
				}
				if (!isEqual(this.ccId, department.ccId)) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "COST_CENTER_ID");
					thisChange.put("OLD_VALUE", department.ccId);
					thisChange.put("NEW_VALUE", this.ccId);
					change.add(thisChange);
				}
				if (!isEqual(this.active + "", department.active + "")) {
					Map<String,String> thisChange = new HashMap<String,String>();
					thisChange.put("COLUMN_NAME", "IS_ACTIVE");
					thisChange.put("OLD_VALUE", department.active?"Y":"N");
					thisChange.put("NEW_VALUE", this.active?"Y":"N");
					change.add(thisChange);
				}
			}

			return change;
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
		 * @return the ccId
		 */
		public String getCcId() {
			return ccId;
		}

		/**
		 * @return the active
		 */
		public boolean isActive() {
			return active;
		}
	}
	
	private static HttpClient createHttpClient() {
		HttpClient httpClient = new DefaultHttpClient();

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
			ClientConnectionManager ccm = httpClient.getConnectionManager();
			SchemeRegistry sr = ccm.getSchemeRegistry();
			sr.register(new Scheme("https", 443, ssf));
			httpClient = new DefaultHttpClient(ccm, httpClient.getParams());
		} catch (Exception e) {
			LogFactory.getLog(MasterDepartmentUpdate.class).error("Exception encountered: " + e.getClass().getName() + "; " + e.getMessage(), e);
		}

		return httpClient;
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
	 * @param prDataSource the prDataSource to set
	 */
	public void setPrDataSource(DataSource prDataSource) {
		this.prDataSource = prDataSource;
	}

	/**
	 * @param psDataSource the psDataSource to set
	 */
	public void setPsDataSource(DataSource psDataSource) {
		this.psDataSource = psDataSource;
	}

	/**
	 * @param up2dateService the up2dateService to set
	 */
	public void setUp2dateService(Up2DateService up2dateService) {
		this.up2dateService = up2dateService;
	}

	/**
	 * @param costCenterService the costCenterService to set
	 */
	public void setCostCenterService(String costCenterService) {
		this.costCenterService = costCenterService;
	}

	/**
	 * @param publisherId the publisherId to set
	 */
	public void setPublisherId(String publisherId) {
		this.publisherId = publisherId;
	}
}
