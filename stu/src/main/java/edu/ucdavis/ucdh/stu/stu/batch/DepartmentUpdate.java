package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Updates the Service Manager department list from the PeopleSoft department list.</p>
 */
public class DepartmentUpdate implements SpringBatchJob {
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
	private Department peopleSoftRecord = null;
	private Department serviceManagerRecord = null;
	private Map<String,String> loginIdByPpsid = new HashMap<String,String>();
	private List<Department> activatePile = new ArrayList<Department>();
	private List<Department> reactivatePile = new ArrayList<Department>();
	private List<Department> updatePile = new ArrayList<Department>();
	private List<Department> deactivatePile = new ArrayList<Department>();
	private List<NameChange> nameChangePile = new ArrayList<NameChange>();
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
//		psRs = psStmt.executeQuery("SELECT DEPTID, DESCR FROM PS_DEPT_TBL A WHERE EFFDT = (SELECT MAX(EFFDT) FROM PS_DEPT_TBL A1 WHERE A.DEPTID = A1.DEPTID AND A.SETID = A1.SETID AND EFFDT <= CURRENT DATE) AND (DEPTID LIKE 'H 100%' OR DEPTID LIKE 'S %') ORDER BY DEPTID");
		psRs = psStmt.executeQuery("SELECT DEPTID, GL_EXPENSE, DESCR, NATIONAL_ID FROM PS_UC_DEP_MGR_VW WHERE GL_EXPENSE > '' ORDER BY DEPTID");

		// connect to ServiceManager database
		Class.forName(smDriver);
		smConn = DriverManager.getConnection(smURL, smUser, smPassword);
		smStmt = smConn.createStatement();
		smRs = smStmt.executeQuery("SELECT DEPT_ID, DEPT_NAME, UCD_DEPT_MGR, UCD_DEPT_CODE, UCD_ACTIVE FROM DEPTM1 ORDER BY DEPT_ID");

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

		// insert new departments
		if (activatePile.size() > 0) {
			log.info("Adding " + activatePile.size() + " new department(s).");
			log.info(" ");
			for (int i=0; i<activatePile.size(); i++) {
				Department thisDepartment = activatePile.get(i);
				PreparedStatement ps = null;
				try {
					ps = smConn.prepareStatement("insert into DEPTM1 (DEPT_ID, DEPT, LAST_UPDATE, UPDATED_BY, SYSMODTIME, EVENT_UPDATED, SYSMODCOUNT, SYSMODUSER, COMPANY, DEPT_STRUCTURE, DEPT_FULL_NAME, [LEVEL], DEPT_NAME, DELFLAG, SLVL, UCD_ACTIVE, UCD_DEPT_MGR, UCD_DEPT_CODE) values(?, ?, GETDATE(), ?, GETDATE(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
					ps.setString(1, thisDepartment.getId());
					ps.setString(2, "UCDHS/" + thisDepartment.getId() + " - " + thisDepartment.getName());
					ps.setString(3, "DepartmentUpdate");
					ps.setString(4, "f");
					ps.setInt(5, 0);
					ps.setString(6, "DepartmentUpdate");
					ps.setString(7, "UCDHS");
					ps.setString(8, thisDepartment.getName());
					ps.setString(9, "UCDHS/" + thisDepartment.getName());
					ps.setInt(10, 0);
					ps.setString(11, thisDepartment.getName());
					ps.setInt(12, 1);
					ps.setInt(13, 2);
					ps.setString(14, thisDepartment.isActive()?"t":"f");
					ps.setString(15, thisDepartment.getManager());
					ps.setString(16, thisDepartment.getAltId());
					if (ps.executeUpdate() > 0) {
						newDepartmentsInserted++;
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
		}

		// reactivate existing departments
		if (reactivatePile.size() > 0) {
			log.info("Reactivating " + reactivatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<reactivatePile.size(); i++) {
				Department thisDepartment = reactivatePile.get(i);
				PreparedStatement ps = null;
				try {
					ps = smConn.prepareStatement("update DEPTM1 set DEPT=?, LAST_UPDATE=GETDATE(), UPDATED_BY='DepartmentUpdate', SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='DepartmentUpdate', DEPT_STRUCTURE=?, DEPT_FULL_NAME=?, DEPT_NAME=?, UCD_ACTIVE=?, UCD_DEPT_MGR=?, UCD_DEPT_CODE=? WHERE DEPT_ID=?");
					ps.setString(1, "UCDHS/" + thisDepartment.getId() + " - " + thisDepartment.getName());
					ps.setString(2, thisDepartment.getName());
					ps.setString(3, "UCDHS/" + thisDepartment.getName());
					ps.setString(4, thisDepartment.getName());
					ps.setString(5, thisDepartment.isActive()?"t":"f");
					ps.setString(6, thisDepartment.getManager());
					ps.setString(7, thisDepartment.getAltId());
					ps.setString(8, thisDepartment.getId());
					if (ps.executeUpdate() > 0) {
						existingDepartmentsReactivated++;
					} else {
						failedUpdates++;
						log.error("Unable to reactivate department " + thisDepartment.getId());
					}
				} catch (Exception e) {
					failedUpdates++;
					log.error("Exception encountered while attempting to reactivate department " + thisDepartment.getId() + "; " + e.getMessage(), e);
				} finally {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		}

		// update the name of existing departments
		if (updatePile.size() > 0) {
			log.info("Updating " + updatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<updatePile.size(); i++) {
				Department thisDepartment = updatePile.get(i);
				PreparedStatement ps = null;
				try {
					ps = smConn.prepareStatement("update DEPTM1 set DEPT=?, LAST_UPDATE=GETDATE(), UPDATED_BY='DepartmentUpdate', SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='DepartmentUpdate', DEPT_STRUCTURE=?, DEPT_FULL_NAME=?, DEPT_NAME=?, UCD_DEPT_MGR=?, UCD_DEPT_CODE=? where DEPT_ID=?");
					ps.setString(1, "UCDHS/" + thisDepartment.getId() + " - " + thisDepartment.getName());
					ps.setString(2, thisDepartment.getName());
					ps.setString(3, "UCDHS/" + thisDepartment.getName());
					ps.setString(4, thisDepartment.getName());
					ps.setString(5, thisDepartment.getManager());
					ps.setString(6, thisDepartment.getAltId());
					ps.setString(7, thisDepartment.getId());
					if (ps.executeUpdate() > 0) {
						existingDepartmentsUpdated++;
					} else {
						failedUpdates++;
						log.error("Unable to update department " + thisDepartment.getId());
					}
				} catch (Exception e) {
					failedUpdates++;
					log.error("Exception encountered while attempting to update department " + thisDepartment.getId() + "; " + e.getMessage(), e);
				} finally {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		}

		// deactivate existing departments
		if (deactivatePile.size() > 0) {
			log.info("Deactivating " + deactivatePile.size() + " existing department(s).");
			log.info(" ");
			for (int i=0; i<deactivatePile.size(); i++) {
				Department thisDepartment = deactivatePile.get(i);
				PreparedStatement ps = null;
				try {
					ps = smConn.prepareStatement("update DEPTM1 set UCD_ACTIVE='f', LAST_UPDATE=GETDATE(), UPDATED_BY='DepartmentUpdate', SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='DepartmentUpdate' where DEPT_ID=?");
					ps.setString(1, thisDepartment.getId());
					if (ps.executeUpdate() > 0) {
						existingDepartmentsDeactivated++;
					} else {
						failedUpdates++;
						log.error("Unable to deactivate department " + thisDepartment.getId());
					}
				} catch (Exception e) {
					failedUpdates++;
					log.error("Exception encountered while attempting to deactivate department " + thisDepartment.getId() + "; " + e.getMessage(), e);
				} finally {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
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
					ps = smConn.prepareStatement("update CONTCTSM1 set DEPT=?, DEPT_NAME=?, SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='DepartmentUpdate' where DEPT_NAME=?");
					ps.setString(1, nameChange.getNewId());
					ps.setString(2, nameChange.getNewName());
					ps.setString(3, nameChange.getOldName());
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
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), peopleSoftRecord.getId(), peopleSoftRecord.getName()));
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
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), peopleSoftRecord.getId(), peopleSoftRecord.getName()));
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
				nameChangePile.add(new NameChange(serviceManagerRecord.getName(), null, DEPT_UNKNOWN));
			}
		}
		getNextServiceManagerRecord();
	}

	/**
	 * <p>Get next peopleSoft record.</p>
	 */
	private void getNextPeopleSoftRecord() throws Exception {
		if (psRs.next()) {
		   	peopleSoftRecord = new Department(psRs.getString("DEPTID"), psRs.getString("DESCR"), convertPpsidToLoginId(psRs.getString("NATIONAL_ID")), psRs.getString("GL_EXPENSE"), true);
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
			serviceManagerRecord = new Department(smRs.getString("DEPT_ID"), smRs.getString("DEPT_NAME"), smRs.getString("UCD_DEPT_MGR"), smRs.getString("UCD_DEPT_CODE"), "t".equalsIgnoreCase(smRs.getString("UCD_ACTIVE")));
			serviceManagerRecordsRead++;
		} else {
			serviceManagerRecord = null;
		}
	}

	/**
	 * <p>Get login ID for PPSID.</p>
	 */
	private String convertPpsidToLoginId(String ppsid) {
		if (!loginIdByPpsid.containsKey(ppsid)) {
			loginIdByPpsid.put(ppsid, fetchLoginId(ppsid));
		}
		return loginIdByPpsid.get(ppsid);
	}

	/**
	 * <p>Fetch login ID for PPSID.</p>
	 */
	private String fetchLoginId(String ppsid) {
		String loginId = "";

		if (StringUtils.isNotEmpty(ppsid)) {
			PreparedStatement ps = null;
			try {
				ps = smConn.prepareStatement("SELECT CONTACT_NAME FROM CONTCTSM1 WHERE USER_ID=? AND ACTIVE='t'");
				ps.setString(1, ppsid);
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					loginId = rs.getString("CONTACT_NAME");
					loginIdsFound++;
				} else {
					loginIdsNotFound++;
					log.error("Login id not found for ppsid " + ppsid);
				}
			} catch (Exception e) {
				loginIdsNotFound++;
				log.error("Exception encountered while attempting to fetch login id for ppsid " + ppsid + "; " + e.getMessage(), e);
			} finally {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}

		return loginId;
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
		private String newId = null;
		private String newName = null;

		public NameChange(String oldName, String newId, String newName) {
			this.oldName = oldName;
			this.newId = newId;
			this.newName = newName;
		}

		/**
		 * @return the oldName
		 */
		public String getOldName() {
			return oldName;
		}

		/**
		 * @return the newId
		 */
		public String getNewId() {
			return newId;
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
}
