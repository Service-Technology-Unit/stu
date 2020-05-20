package edu.ucdavis.ucdh.stu.stu.servlets;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.snutil.servlets.SubscriberServlet;
import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

/**
 * <p>This servlet processes a single add/change/delete action for IAM persons.</p>
 */
public class PersonUpdateServlet extends SubscriberServlet {
	private static final long serialVersionUID = 1;
	private static final String EXT_FETCH_URL = "/api/now/table/x_ucdhs_identity_s_identity?sysparm_fields=sys_id%2Cnumber%2Cemployee_number%2Cexternal_id%2Cstart_date%2Cend_date%2Ctitle%2Csupervisor.employee_number%2Cdepartment.u_id_6%2Cphone%2Clocation.u_location_code%2Cemail&sysparm_query=ORDERBYDESCsys_created_on%5Eemployee_number%3D";
	private static final String EXT_FETCH_URL2 = "/api/now/table/x_ucdhs_identity_s_identity?sysparm_fields=sys_id%2Cnumber%2Cemployee_number%2Cexternal_id%2Cstart_date%2Cend_date%2Ctitle%2Csupervisor.employee_number%2Cdepartment.u_id_6%2Cphone%2Clocation.u_location_code%2Cemail&sysparm_query=ORDERBYDESCsys_created_on%5Eexternal_id%3D";
	private static final String EXT_VERIFY_URL = "/api/now/table/x_ucdhs_identity_s_identity?sysparm_fields=start_date%2Cend_date&sysparm_query=ORDERBYDESCsys_created_on%5Eexternal_id%3D";
	private static final String FETCH_SQL = "SELECT ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, SUPERVISOR_NAME, MANAGER, MANAGER_NAME, DEPT_ID, DEPT_NAME, HS_AD_ID, HS_AD_ID_CT, KERBEROS_ID, KERBEROS_ID_CT, PPS_ID, PPS_ID_CT, EXTERNAL_ID, EXTERNAL_ID_CT, MOTHRA_ID, MOTHRA_ID_CT, BANNER_ID, BANNER_ID_CT, STUDENT_ID, STUDENT_ID_CT, VOLUNTEER_ID, VOLUNTEER_ID_CT, CAMPUS_PPS_ID, CAMPUS_PPS_ID_CT, UC_PATH_ID, UC_PATH_ID_CT, IS_ACTIVE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_PREVIOUS_HS_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, LOCATION_NAME, ADDRESS, CITY, STATE, ZIP, BUILDING, FLOOR, ROOM, STATION, CUBE, UC_PATH_INSTITUTION, UC_PATH_TYPE, UC_PATH_PERCENT, UC_PATH_REPRESENTATION, STUDENT_MAJOR, STUDENT_MAJOR_NAME, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM, SYSMODCOUNT, SYSMODTIME, SYSMODUSER, SYSMODADDR FROM VIEW_PERSON_ALL WHERE ID=?";
	private static final String USER_ID = "up2date";
	private static final String[] FIELD = {"ID", "LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "TITLE", "SUPERVISOR", "MANAGER", "DEPT_ID", "HS_AD_ID", "KERBEROS_ID", "PPS_ID", "EXTERNAL_ID", "MOTHRA_ID", "BANNER_ID", "STUDENT_ID", "VOLUNTEER_ID", "CAMPUS_PPS_ID", "UC_PATH_ID", "IS_ACTIVE", "IS_UCDH_EMPLOYEE", "IS_UCD_EMPLOYEE", "IS_EXTERNAL", "IS_PREVIOUS_UCDH_EMPLOYEE", "IS_PREVIOUS_UCD_EMPLOYEE", "IS_STUDENT", "START_DATE", "END_DATE", "PHONE_NUMBER", "CELL_NUMBER", "PAGER_NUMBER", "PAGER_PROVIDER", "ALTERNATE_PHONES", "EMAIL", "ALTERNATE_EMAIL", "LOCATION_CODE", "STATION", "UC_PATH_INSTITUTION", "UC_PATH_TYPE", "UC_PATH_PERCENT", "UC_PATH_REPRESENTATION", "STUDENT_MAJOR", "STUDENT_MAJOR_NAME", "CREATED_ON", "CREATED_BY", "CREATED_FROM", "UPDATE_CT", "UPDATED_ON", "UPDATED_BY", "UPDATED_FROM"};
	private static final String[] ID_FIELD = {"IS_ACTIVE","IS_PRIMARY","START_DATE","END_DATE"};
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	private Log log = LogFactory.getLog(getClass());
	private String publisherId = null;
	private Boolean idVerification = null;
	private Up2DateService up2dateService = null;
	private DataSource dataSource = null;
	private List<Map<String,String>> field = new ArrayList<Map<String,String>>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		idVerification = (Boolean) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("verifyIdFlag");
		publisherId = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("personPublisherId");
		up2dateService = (Up2DateService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("up2dateService");
		Hashtable<String,String> parms = new Hashtable<String,String>();
		try {
			Context ctx = new InitialContext(parms);
			dataSource = (DataSource) ctx.lookup("java:comp/env/jdbc/masterdata");
		} catch (Exception e) {
			log.error("Exception encountered attempting to obtain datasource jdbc/masterdata", e);
			eventService.logEvent(new Event("java:comp/env/jdbc/masterdata", "Datasource lookup error", "Exception encountered attempting to obtain datasource jdbc/masterdata: " + e, null, e));
		}
		establsihFieldInformation();
	}

	/**
	 * <p>Processes the incoming request.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the outcome of the request processing
	 */
	@SuppressWarnings("unchecked")
	protected String processRequest(HttpServletRequest req, JSONObject details) {
		String response = "";

		String action = req.getParameter("_action");
		String id = req.getParameter("id");

		if (StringUtils.isNotEmpty(id)) {
			JSONObject newPerson = buildPersonFromRequest(req, details);
			details.put("newPerson", newPerson);
			JSONObject oldPerson = fetchPerson(id, details);
			if (oldPerson != null) {
				details.put("oldPerson", oldPerson);
				if (!action.equalsIgnoreCase("force") && personUnchanged(req, newPerson, oldPerson)) {
					response = "1;No action taken -- no changes detected";
				} else {
					response = updatePerson(req, newPerson, oldPerson, details);
				}
			} else {
				if (action.equalsIgnoreCase("delete")) {
					response = "1;No action taken -- person not on file";
				} else {
					if ("Y".equalsIgnoreCase(req.getParameter("isUcdhEmployee")) || StringUtils.isNotEmpty(req.getParameter("adId")) || (StringUtils.isNotEmpty(req.getParameter("externalId")) && req.getParameter("externalId").startsWith("H"))) {
						response = insertPerson(req, newPerson, details);
					} else {
						response = "1;No action taken -- not a Health System person";
					}
				}
			}
		} else {
			response = "2;Error - Required parameter \"id\" has no value";
		}

		if (response.startsWith("0;") || "force".equalsIgnoreCase(action)) {
			String publishAction = action;
			if (!"force".equalsIgnoreCase(action)) {
				publishAction = "change";
				if (response.indexOf("inserted") != -1) {
					publishAction = "add";
				} else if (response.indexOf("deactivated") != -1) {
					publishAction = "delete";
				}
			}
			publishUpdate(id, publishAction, details);
		}

		return response;
	}

	/**
	 * <p>Fetches the person data from the database.</p>
	 *
	 * @param personId the ID of the person
	 * @return the person data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchPerson(String personId, JSONObject details) {
		JSONObject person = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching person for personId " + personId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			String sql = "SELECT ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, HS_AD_ID, KERBEROS_ID, PPS_ID, EXTERNAL_ID, MOTHRA_ID, BANNER_ID, STUDENT_ID, VOLUNTEER_ID, CAMPUS_PPS_ID, UC_PATH_ID, IS_ACTIVE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, STATION, UC_PATH_INSTITUTION, UC_PATH_TYPE, UC_PATH_PERCENT, UC_PATH_REPRESENTATION, STUDENT_MAJOR, STUDENT_MAJOR_NAME, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM FROM VIEW_PERSON_ALL WHERE ID=?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, personId);
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Person " + personId + " found");
				}
				person = new JSONObject();
				for (int i=0; i<FIELD.length; i++) {
					person.put(FIELD[i], rs.getString(FIELD[i]));
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Person " + personId + " not found");
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching person " + personId + ": " + e.getMessage(), e);
			eventService.logEvent(new Event(personId, "Person fetch exception", "Exception encountered fetching person " + personId + ": " + e.getMessage(), details, e));
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
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning person: " + person);
		}

		return person;
	}

	/**
	 * <p>Fetches the person's supervisor from the database.</p>
	 *
	 * @param personId the ID of the person
	 * @return the person's supervisor's IAM ID
	 */
	private String getSupervisor(String personId, JSONObject details) {
		String supervisor = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching supervisor for personId " + personId);
		}
		if (StringUtils.isNotEmpty(personId)) {
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				conn = dataSource.getConnection();
				String sql = "SELECT SUPERVISOR FROM PERSON WHERE ID=?";
				ps = conn.prepareStatement(sql);
				ps.setString(1, personId);
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + sql);
				}
				rs = ps.executeQuery();
				if (rs.next()) {
					if (log.isDebugEnabled()) {
						log.debug("Person " + personId + " found");
					}
					supervisor = rs.getString("SUPERVISOR");
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Person " + personId + " not found");
					}
				}
			} catch (SQLException e) {
				log.error("Exception encountered fetching person " + personId + ": " + e.getMessage(), e);
				eventService.logEvent(new Event(personId, "Supervisor fetch exception", "Exception encountered fetching supervisor " + personId + ": " + e.getMessage(), details, e));
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
				if (conn != null) {
					try {
						conn.close();
					} catch (Exception e) {
						//
					}
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning supervisor: " + supervisor);
		}

		return supervisor;
	}

	/**
	 * <p>Checks to see if the data of a person has changed using the following values.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param person the original person data
	 * @return true if the person is unchanged
	 */
	private boolean personUnchanged(HttpServletRequest req, JSONObject newPerson, JSONObject oldPerson) {
		boolean unchanged = false;

		if (isEqual(oldPerson, newPerson, "BANNER_ID") &&
				isEqual(oldPerson, newPerson, "CAMPUS_PPS_ID") &&
				isEqual(oldPerson, newPerson, "DEPT_ID") &&
				isEqual(oldPerson, newPerson, "EMAIL") &&
				isEqual(oldPerson, newPerson, "ALTERNATE_EMAIL") &&
				isEqual(oldPerson, newPerson, "END_DATE") &&
				isEqual(oldPerson, newPerson, "EXTERNAL_ID") &&
				isEqual(oldPerson, newPerson, "FIRST_NAME") &&
				isEqual(oldPerson, newPerson, "HS_AD_ID") &&
				isEqual(oldPerson, newPerson, "ID") &&
				isEqual(oldPerson, newPerson, "IS_ACTIVE") &&
				isEqual(oldPerson, newPerson, "IS_EXTERNAL") &&
				isEqual(oldPerson, newPerson, "IS_PREVIOUS_UCD_EMPLOYEE") &&
				isEqual(oldPerson, newPerson, "IS_PREVIOUS_UCDH_EMPLOYEE") &&
				isEqual(oldPerson, newPerson, "IS_STUDENT") &&
				isEqual(oldPerson, newPerson, "IS_UCD_EMPLOYEE") &&
				isEqual(oldPerson, newPerson, "IS_UCDH_EMPLOYEE") &&
				isEqual(oldPerson, newPerson, "KERBEROS_ID") &&
				isEqual(oldPerson, newPerson, "LAST_NAME") &&
				isEqual(oldPerson, newPerson, "LOCATION_CODE") &&
				isEqual(oldPerson, newPerson, "MANAGER") &&
				isEqual(oldPerson, newPerson, "MIDDLE_NAME") &&
				isEqual(oldPerson, newPerson, "MOTHRA_ID") &&
				isEqual(oldPerson, newPerson, "PHONE_NUMBER") &&
				isEqual(oldPerson, newPerson, "PPS_ID") &&
				isEqual(oldPerson, newPerson, "START_DATE") &&
				isEqual(oldPerson, newPerson, "STUDENT_ID") &&
				isEqual(oldPerson, newPerson, "STUDENT_MAJOR") &&
				isEqual(oldPerson, newPerson, "STUDENT_MAJOR_NAME") &&
				isEqual(oldPerson, newPerson, "SUPERVISOR") &&
				isEqual(oldPerson, newPerson, "TITLE") &&
				isEqual(oldPerson, newPerson, "UC_PATH_ID") &&
				isEqual(oldPerson, newPerson, "UC_PATH_INSTITUTION") &&
				isEqual(oldPerson, newPerson, "UC_PATH_PERCENT") &&
				isEqual(oldPerson, newPerson, "UC_PATH_REPRESENTATION") &&
				isEqual(oldPerson, newPerson, "UC_PATH_TYPE")) {
			unchanged = true;
		}

		return unchanged;
	}

	/**
	 * <p>Inserts a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the response string
	 */
	@SuppressWarnings("unchecked")
	private String insertPerson(HttpServletRequest req, JSONObject person, JSONObject details) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Inserting person " + req.getParameter("id"));
		}

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		person.put("START_DATE", getPersonStartDate(person, null));
		if (StringUtils.isEmpty((String) person.get("UC_PATH_ID")) || "UCD".equalsIgnoreCase((String) person.get("UC_PATH_INSTITUTION"))) {
			// discard UCD campus department
			person.remove("DEPT_ID");
			person = addExternalData(person, null, details);
		}
		if (StringUtils.isEmpty((String) person.get("MANAGER"))) {
			person.put("MANAGER", getSupervisor((String) person.get("SUPERVISOR"), details));
			if (log.isDebugEnabled()) {
				log.debug("Manager from supervisor's supervisor: " + person.get("MANAGER"));
			}
		}
		if (StringUtils.isEmpty((String) person.get("MANAGER"))) {
			if (StringUtils.isNotEmpty((String) person.get("DEPT_ID"))) {
				person.put("MANAGER", getDepartmentManager((String) person.get("DEPT_ID"), details));
				if (log.isDebugEnabled()) {
					log.debug("Manager from getDepartmentManager: " + person.get("MANAGER"));
				}
			}
		}
		boolean personActive = false;
		boolean ucdhEmployee = false;
		boolean ucdhExternal = false;
		String startDate = getCurrentDate();
		JSONObject ids = new JSONObject();
		if (StringUtils.isNotEmpty((String) person.get("UC_PATH_ID"))) {
			String idType = "UC PATH";
			ids = addUpdateId(ids, idType, (String) person.get("UC_PATH_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			if (idVerification) {
				thisId.put("IS_PRIMARY", "N");
				verifyId(idType, thisId, person, details);
			}
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				personActive = true;
				thisId.put("IS_PRIMARY", "Y");
				if ("UCDH".equalsIgnoreCase((String) person.get("UC_PATH_INSTITUTION"))) {
					ucdhEmployee = true;
					person.put("IS_UCDH_EMPLOYEE", "Y");
					person.put("IS_UCD_EMPLOYEE", "N");
				} else {
					person.put("IS_UCDH_EMPLOYEE", "N");
					person.put("IS_UCD_EMPLOYEE", "Y");
				}
			} else {
				if ("UCDH".equalsIgnoreCase((String) person.get("UC_PATH_INSTITUTION"))) {
					person.put("IS_PREVIOUS_UCDH_EMPLOYEE", "Y");
				} else {
					person.put("IS_PREVIOUS_UCD_EMPLOYEE", "Y");
				}
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("MOTHRA_ID"))) {
			String idType = "MOTHRA";
			ids = addUpdateId(ids, idType, (String) person.get("MOTHRA_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("HS_AD_ID"))) {
			if (log.isDebugEnabled()) {
				log.debug("ID Verification Value: " + idVerification);
			}
			String idType = "HS A/D";
			ids = addUpdateId(ids, idType, (String) person.get("HS_AD_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			if (idVerification) {
				thisId.put("IS_PRIMARY", "N");
				verifyId(idType, thisId, person, details);
				if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
					thisId.put("IS_PRIMARY", "Y");
				}
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("KERBEROS_ID"))) {
			String idType = "KERBEROS";
			ids = addUpdateId(ids, idType, (String) person.get("KERBEROS_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("PPS_ID"))) {
			String idType = "PPS";
			ids = addUpdateId(ids, idType, (String) person.get("PPS_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("CAMPUS_PPS_ID"))) {
			String idType = "CAMPUS PPS";
			ids = addUpdateId(ids, idType, (String) person.get("CAMPUS_PPS_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("EXTERNAL_ID"))) {
			String idType = "EXTERNAL";
			ids = addUpdateId(ids, idType, (String) person.get("EXTERNAL_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				if (((String) thisId.get("ID_VALUE")).startsWith("H")) {
					personActive = true;
					ucdhExternal = true;
					person.put("IS_EXTERNAL", "Y");
				}
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("BANNER_ID"))) {
			String idType = "BANNER";
			ids = addUpdateId(ids, idType, (String) person.get("BANNER_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("STUDENT_ID"))) {
			String idType = "STUDENT";
			ids = addUpdateId(ids, idType, (String) person.get("STUDEN, detailsT_ID"), person, details);
			JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
			thisId.put("IS_PRIMARY", "N");
			verifyId(idType, thisId, person, details);
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				thisId.put("IS_PRIMARY", "Y");
			}
		}
		if (personActive) {
			if (ucdhEmployee) {
				startDate = (String) ((JSONObject) ((JSONArray) ids.get("UC PATH")).get(0)).get("START_DATE");
			} else if (ucdhExternal) {
				startDate = (String) ((JSONObject) ((JSONArray) ids.get("EXTERNAL")).get(0)).get("START_DATE");
			}
		}
		Connection conn = null;
		PreparedStatement ps = null;
		String sql = "INSERT INTO PERSON (ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, IS_ACTIVE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_STUDENT, START_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, STATION, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, 'Y', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, 0, getdate(), ?, ?)";
		if (!personActive) {
			sql = "INSERT INTO PERSON (ID, LAST_NAME, FIRST_NAME, MIDDLE_NAME, TITLE, SUPERVISOR, MANAGER, DEPT_ID, IS_ACTIVE, IS_UCDH_EMPLOYEE, IS_UCD_EMPLOYEE, IS_EXTERNAL, IS_PREVIOUS_UCDH_EMPLOYEE, IS_PREVIOUS_UCD_EMPLOYEE, IS_STUDENT, START_DATE, END_DATE, PHONE_NUMBER, CELL_NUMBER, PAGER_NUMBER, PAGER_PROVIDER, ALTERNATE_PHONES, EMAIL, ALTERNATE_EMAIL, LOCATION_CODE, STATION, CREATED_ON, CREATED_BY, CREATED_FROM, UPDATE_CT, UPDATED_ON, UPDATED_BY, UPDATED_FROM) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, 'N', ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate(), ?, ?, 0, getdate(), ?, ?)";
		}
		try {
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, (String) person.get("ID"));
			ps.setString(2, (String) person.get("LAST_NAME"));
			ps.setString(3, (String) person.get("FIRST_NAME"));
			ps.setString(4, (String) person.get("MIDDLE_NAME"));
			ps.setString(5, (String) person.get("TITLE"));
			ps.setString(6, (String) person.get("SUPERVISOR"));
			ps.setString(7, (String) person.get("MANAGER"));
			ps.setString(8, (String) person.get("DEPT_ID"));
			ps.setString(9, (String) person.get("IS_UCDH_EMPLOYEE"));
			ps.setString(10, (String) person.get("IS_UCD_EMPLOYEE"));
			ps.setString(11, (String) person.get("IS_EXTERNAL"));
			ps.setString(12, (String) person.get("IS_PREVIOUS_UCDH_EMPLOYEE"));
			ps.setString(13, (String) person.get("IS_PREVIOUS_UCD_EMPLOYEE"));
			ps.setString(14, (String) person.get("IS_STUDENT"));
			ps.setString(15, startDate);
			ps.setString(16, (String) person.get("PHONE_NUMBER"));
			ps.setString(17, (String) person.get("CELL_NUMBER"));
			ps.setString(18, (String) person.get("PAGER_NUMBER"));
			ps.setString(19, (String) person.get("PAGER_PROVIDER"));
			ps.setString(20, (String) person.get("ALTERNATE_PHONES"));
			ps.setString(21, (String) person.get("EMAIL"));
			ps.setString(22, (String) person.get("ALTERNATE_EMAIL"));
			ps.setString(23, (String) person.get("LOCATION_CODE"));
			ps.setString(24, (String) person.get("STATION"));
			ps.setString(25, USER_ID);
			ps.setString(26, remoteAddr);
			ps.setString(27, USER_ID);
			ps.setString(28, remoteAddr);
			if (ps.executeUpdate() > 0) {
				boolean success = true;
				if (log.isDebugEnabled()) {
					log.debug("Person successfully inserted");
				}
				Iterator<String> i = ids.keySet().iterator();
				while (i.hasNext()) {
					String idType = i.next();
					JSONObject thisId = (JSONObject) ((JSONArray) ids.get(idType)).get(0);
					if (!insertPersonID(conn, (String) person.get("ID"), idType, thisId, remoteAddr, details)) {
						success = false;
					}
				}
				if (success) {
					response = "0;Person inserted";
				} else {
					response = "2;Unable to insert one or more person IDs";
				}
			} else {
				response = "2;Unable to insert person";
				log.error("Unable to insert person " + person.get("ID"));
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to insert person: " + e.toString();
			log.error("Exception encountered while attempting to insert person " + person.get("ID") + "; " + e.getMessage(), e);
			eventService.logEvent(new Event((String) person.get("ID"), "Person insert exception", "Exception encountered while attempting to insert person " + person.get("ID") + "; " + e.getMessage(), details, e));
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

		return response;
	}

	/**
	 * <p>Updates the person data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newPerson the new values for the person
	 * @param oldPerson the old values for the person
	 * @return the response string
	 */
	@SuppressWarnings("unchecked")
	private String updatePerson(HttpServletRequest req, JSONObject newPerson, JSONObject oldPerson, JSONObject details) {
		String response = "";

		String id = req.getParameter("id");
		if (log.isDebugEnabled()) {
			log.debug("Updating person " + id);
		}

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		newPerson.put("START_DATE", getPersonStartDate(newPerson, oldPerson));
		if (StringUtils.isEmpty((String) newPerson.get("UC_PATH_ID")) || "UCD".equalsIgnoreCase((String) newPerson.get("UC_PATH_INSTITUTION"))) {
			// discard UCD campus department
			newPerson.remove("DEPT_ID");
			newPerson = addExternalData(newPerson, oldPerson, details);
		}
		if (StringUtils.isEmpty((String) newPerson.get("DEPT_ID"))) {
			if (StringUtils.isNotEmpty((String) oldPerson.get("DEPT_ID"))) {
				newPerson.put("DEPT_ID", oldPerson.get("DEPT_ID"));
			}
		}
		if (StringUtils.isEmpty((String) newPerson.get("SUPERVISOR"))) {
			newPerson.put("SUPERVISOR", oldPerson.get("SUPERVISOR"));
			if (log.isDebugEnabled()) {
				log.debug("Supervisor from oldPerson: " + newPerson.get("SUPERVISOR"));
			}
		}
		if (StringUtils.isEmpty((String) newPerson.get("MANAGER"))) {
			newPerson.put("MANAGER", getSupervisor((String) newPerson.get("SUPERVISOR"), details));
			if (log.isDebugEnabled()) {
				log.debug("Manager from supervisor's supervisor: " + newPerson.get("MANAGER"));
			}
		}
		if (StringUtils.isEmpty((String) newPerson.get("MANAGER"))) {
			if (StringUtils.isNotEmpty((String) newPerson.get("DEPT_ID"))) {
				newPerson.put("MANAGER", getDepartmentManager((String) newPerson.get("DEPT_ID"), details));
				if (log.isDebugEnabled()) {
					log.debug("Manager from getDepartmentManager: " + newPerson.get("MANAGER"));
				}
			}
		}
		if (StringUtils.isEmpty((String) newPerson.get("MANAGER"))) {
			newPerson.put("MANAGER", oldPerson.get("MANAGER"));
			if (log.isDebugEnabled()) {
				log.debug("Manager from oldPerson: " + newPerson.get("MANAGER"));
			}
		}

		// get existing IDs
		JSONObject ids = fetchIds(id, details);

		// add/update IDs
		if (StringUtils.isNotEmpty((String) newPerson.get("UC_PATH_ID"))) {
			ids = addUpdateId(ids, "UC PATH", (String) newPerson.get("UC_PATH_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("MOTHRA_ID"))) {
			ids = addUpdateId(ids, "MOTHRA", (String) newPerson.get("MOTHRA_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("HS_AD_ID"))) {
			ids = addUpdateId(ids, "HS A/D", (String) newPerson.get("HS_AD_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("KERBEROS_ID"))) {
			ids = addUpdateId(ids, "KERBEROS", (String) newPerson.get("KERBEROS_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("PPS_ID"))) {
			ids = addUpdateId(ids, "PPS", (String) newPerson.get("PPS_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("CAMPUS_PPS_ID"))) {
			ids = addUpdateId(ids, "CAMPUS PPS", (String) newPerson.get("CAMPUS_PPS_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("EXTERNAL_ID"))) {
			ids = addUpdateId(ids, "EXTERNAL", (String) newPerson.get("EXTERNAL_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("BANNER_ID"))) {
			ids = addUpdateId(ids, "BANNER", (String) newPerson.get("BANNER_ID"), newPerson, details);
		}
		if (StringUtils.isNotEmpty((String) newPerson.get("STUDENT_ID"))) {
			ids = addUpdateId(ids, "STUDENT", (String) newPerson.get("STUDENT_ID"), newPerson, details);
		}

		// verify IDs
		boolean personActive = false;
		Iterator<String> i = ids.keySet().iterator();
		while (i.hasNext()) {
			String idType = i.next();
			if (log.isDebugEnabled()) {
				log.debug("Verifying " + idType + " IDs for " + id);
			}
			JSONArray idList = (JSONArray) ids.get(idType);
			int activeCt = 0;
			int currentCt = 0;
			Iterator<JSONObject> j = idList.iterator();
			while (j.hasNext()) {
				JSONObject thisId = j.next();
				if (("HS A/D".equalsIgnoreCase(idType) || "UC PATH".equalsIgnoreCase(idType))  && !idVerification) {
					if (log.isDebugEnabled()) {
						log.debug("Skipping " + idType + " ID verification");
					}
					activeCt++;
				} else {
					thisId.put("IS_PRIMARY", "N");
					verifyId(idType, thisId, newPerson, details);
					if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
						activeCt++;
						if ("Y".equalsIgnoreCase((String) thisId.get("IS_CURRENT"))) {
							currentCt++;
						}
					}
				}
			}
			if (activeCt > 0) {
				if ("UC PATH".equalsIgnoreCase(idType)) {
					personActive = true;
					if ("UCDH".equalsIgnoreCase((String) newPerson.get("UC_PATH_INSTITUTION"))) {
						newPerson.put("IS_UCDH_EMPLOYEE", "Y");
						newPerson.put("IS_UCD_EMPLOYEE", "N");
					} else {
						newPerson.put("IS_UCDH_EMPLOYEE", "N");
						newPerson.put("IS_UCD_EMPLOYEE", "Y");
					}
				}
				if (activeCt == 1) {
					j = idList.iterator();
					while (j.hasNext()) {
						JSONObject thisId = j.next();
						if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
							thisId.put("IS_PRIMARY", "Y");
							if ("EXTERNAL".equalsIgnoreCase(idType)) {
								if (((String) thisId.get("ID_VALUE")).startsWith("H")) {
									personActive = true;
									newPerson.put("IS_EXTERNAL", "Y");
								}
							}
						}
					}
				} else {
					if (currentCt == 1) {
						j = idList.iterator();
						while (j.hasNext()) {
							JSONObject thisId = j.next();
							if ("Y".equalsIgnoreCase((String) thisId.get("IS_CURRENT"))) {
								thisId.put("IS_PRIMARY", "Y");
								if ("EXTERNAL".equalsIgnoreCase(idType)) {
									if (((String) thisId.get("ID_VALUE")).startsWith("H")) {
										personActive = true;
										newPerson.put("IS_EXTERNAL", "Y");
									}
								}
							}
						}
					} else {
						int primary = -1;
						String latestStart = "";
						for (int x=0; x<idList.size(); x++) {
							JSONObject thisId = (JSONObject) idList.get(x);
							if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
								if ("EXTERNAL".equalsIgnoreCase(idType)) {
									if (((String) thisId.get("ID_VALUE")).startsWith("H")) {
										personActive = true;
										newPerson.put("IS_EXTERNAL", "Y");
									}
								}
								String start = (String) thisId.get("START_DATE");
								if (latestStart.compareTo(start) < 0) {
									primary = x;
									latestStart = start;
								}
							}
						}
						JSONObject thisId = (JSONObject) idList.get(primary);
						thisId.put("IS_PRIMARY", "Y");
					}
				}
			}
		}

		// update database
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = dataSource.getConnection();
			String sql = "UPDATE PERSON SET DEPT_ID=?, EMAIL=?, END_DATE=null, FIRST_NAME=?, IS_ACTIVE='Y', IS_EXTERNAL=?, IS_PREVIOUS_UCD_EMPLOYEE=?, IS_PREVIOUS_UCDH_EMPLOYEE=?, IS_STUDENT=?, IS_UCD_EMPLOYEE=?, IS_UCDH_EMPLOYEE=?, LAST_NAME=?, LOCATION_CODE=?, MANAGER=?, MIDDLE_NAME=?, PHONE_NUMBER=?, START_DATE=?, STUDENT_MAJOR=?, STUDENT_MAJOR_NAME=?, SUPERVISOR=?, TITLE=?, UC_PATH_INSTITUTION=?, UC_PATH_PERCENT=?, UC_PATH_REPRESENTATION=?, UC_PATH_TYPE=?, ALTERNATE_EMAIL=?, UPDATE_CT=UPDATE_CT+1, UPDATED_BY=?, UPDATED_ON=getdate(), UPDATED_FROM=? WHERE ID=?";
			if (!personActive) {
				sql = "UPDATE PERSON SET DEPT_ID=?, EMAIL=?, END_DATE=getdate(), FIRST_NAME=?, IS_ACTIVE='N', IS_EXTERNAL=?, IS_PREVIOUS_UCD_EMPLOYEE=?, IS_PREVIOUS_UCDH_EMPLOYEE=?, IS_STUDENT=?, IS_UCD_EMPLOYEE=?, IS_UCDH_EMPLOYEE=?, LAST_NAME=?, LOCATION_CODE=?, MANAGER=?, MIDDLE_NAME=?, PHONE_NUMBER=?, START_DATE=?, STUDENT_MAJOR=?, STUDENT_MAJOR_NAME=?, SUPERVISOR=?, TITLE=?, UC_PATH_INSTITUTION=?, UC_PATH_PERCENT=?, UC_PATH_REPRESENTATION=?, UC_PATH_TYPE=?, ALTERNATE_EMAIL=?, UPDATE_CT=UPDATE_CT+1, UPDATED_BY=?, UPDATED_ON=getdate(), UPDATED_FROM=? WHERE ID=?";
			}
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, (String) newPerson.get("DEPT_ID"));
			ps.setString(2, (String) newPerson.get("EMAIL"));
			ps.setString(3, (String) newPerson.get("FIRST_NAME"));
			ps.setString(4, (String) newPerson.get("IS_EXTERNAL"));
			ps.setString(5, (String) newPerson.get("IS_PREVIOUS_UCD_EMPLOYEE"));
			ps.setString(6, (String) newPerson.get("IS_PREVIOUS_UCDH_EMPLOYEE"));
			ps.setString(7, (String) newPerson.get("IS_STUDENT"));
			ps.setString(8, (String) newPerson.get("IS_UCD_EMPLOYEE"));
			ps.setString(9, (String) newPerson.get("IS_UCDH_EMPLOYEE"));
			ps.setString(10, (String) newPerson.get("LAST_NAME"));
			ps.setString(11, (String) newPerson.get("LOCATION_CODE"));
			ps.setString(12, (String) newPerson.get("MANAGER"));
			ps.setString(13, (String) newPerson.get("MIDDLE_NAME"));
			ps.setString(14, (String) newPerson.get("PHONE_NUMBER"));
			ps.setString(15, (String) newPerson.get("START_DATE"));
			ps.setString(16, (String) newPerson.get("STUDENT_MAJOR"));
			ps.setString(17, (String) newPerson.get("STUDENT_MAJOR_NAME"));
			ps.setString(18, (String) newPerson.get("SUPERVISOR"));
			ps.setString(19, (String) newPerson.get("TITLE"));
			ps.setString(20, (String) newPerson.get("UC_PATH_INSTITUTION"));
			ps.setString(21, (String) newPerson.get("UC_PATH_PERCENT"));
			ps.setString(22, (String) newPerson.get("UC_PATH_REPRESENTATION"));
			ps.setString(23, (String) newPerson.get("UC_PATH_TYPE"));
			ps.setString(24, (String) newPerson.get("ALTERNATE_EMAIL"));
			ps.setString(25, USER_ID);
			ps.setString(26, remoteAddr);
			ps.setString(27, (String) newPerson.get("ID"));
			if (ps.executeUpdate() > 0) {
				if (log.isDebugEnabled()) {
					log.debug("Person successfully updated");
				}
				updatePersonHistory(req, newPerson, oldPerson, details);
				boolean success = true;
				i = ids.keySet().iterator();
				while (i.hasNext()) {
					String idType = i.next();
					if (log.isDebugEnabled()) {
						log.debug("Updating " + idType + " IDs for " + id);
					}
					JSONArray idList = (JSONArray) ids.get(idType);
					Iterator<JSONObject> j = idList.iterator();
					while (j.hasNext()) {
						JSONObject thisId = j.next();
						if ("New".equalsIgnoreCase((String) thisId.get("ID"))) {
							if (!insertPersonID(conn, id, idType, thisId, remoteAddr, details)) {
								success = false;
							}
						} else {
							boolean idHasChanged = false;
							for (int x=0; x<ID_FIELD.length; x++) {
								String thisFieldName = ID_FIELD[x];
								if (!isEqual2((String) thisId.get("ORIG_" + thisFieldName), (String) thisId.get(thisFieldName))) {
									idHasChanged = true;
								}
							}
							if (idHasChanged) {
								if (!updatePersonID(conn, id, idType, thisId, remoteAddr, details)) {
									success = false;
								}
							}
						}
					}
				}
				if (success) {
					if ("Y".equalsIgnoreCase((String) oldPerson.get("IS_ACTIVE")) && !personActive) {
						response = "0;Person deactivated";
					} else {
						response = "0;Person updated";
					}
				} else {
					response = "2;Unable to update one or more person IDs";
				}
			} else {
				response = "2;Unable to update person";
				log.error("Unable to update person " + req.getParameter("id"));
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to update person: " + e.toString();
			log.error("Exception encountered while attempting to update person " + id + "; " + e.getMessage(), e);
			eventService.logEvent(new Event(id, "Update Person Exception", "Exception encountered while attempting to update person " + id + "; " + e.getMessage(), details, e));
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

		return response;
	}

	/**
	 * <p>Updates the person history for any column updated.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param newPerson the new values for the person
	 * @param oldPerson the old values for the person
	 */
	private void updatePersonHistory(HttpServletRequest req, JSONObject newPerson, JSONObject oldPerson, JSONObject details) {
		String id = req.getParameter("id");
		if (log.isDebugEnabled()) {
			log.debug("Updating person history for person " + id);
		}

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		// find changed columns
		List<Map<String,String>> change = new ArrayList<Map<String,String>>();
		String[] fieldName = {"LAST_NAME", "FIRST_NAME", "MIDDLE_NAME", "TITLE", "SUPERVISOR", "MANAGER", "DEPT_ID", "IS_ACTIVE", "IS_UCDH_EMPLOYEE", "IS_UCD_EMPLOYEE", "IS_EXTERNAL", "IS_PREVIOUS_UCDH_EMPLOYEE", "IS_PREVIOUS_UCD_EMPLOYEE", "IS_STUDENT", "START_DATE", "END_DATE", "PHONE_NUMBER", "CELL_NUMBER", "PAGER_NUMBER", "PAGER_PROVIDER", "ALTERNATE_PHONES", "EMAIL", "ALTERNATE_EMAIL", "LOCATION_CODE", "STATION", "UC_PATH_INSTITUTION", "UC_PATH_TYPE", "UC_PATH_PERCENT", "UC_PATH_REPRESENTATION", "STUDENT_MAJOR", "STUDENT_MAJOR_NAME"};
		for (int i=0; i<fieldName.length; i++) {
			String thisFieldName = fieldName[i];
			if (!isEqual(oldPerson, newPerson, thisFieldName)) {
				Map<String,String> thisChange = new HashMap<String,String>();
				thisChange.put("COLUMN_NAME", thisFieldName);
				thisChange.put("OLD_VALUE", (String) oldPerson.get(thisFieldName));
				thisChange.put("NEW_VALUE", (String) newPerson.get(thisFieldName));
				change.add(thisChange);
			}
		}

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for person " + id);
			}
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = dataSource.getConnection();
				String sql = "INSERT INTO PERSON_HISTORY (PERSON_ID, COLUMN_NAME, OLD_VALUE, NEW_VALUE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES(?, ?, ?, ?, getdate(), ?, 0, ?)";
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + sql);
				}
				ps = conn.prepareStatement(sql);
				Iterator<Map<String,String>> i = change.iterator();
				while (i.hasNext()) {
					Map<String,String> thisChange = i.next();
					ps.setString(1, id);
					ps.setString(2, thisChange.get("COLUMN_NAME"));
					ps.setString(3, thisChange.get("OLD_VALUE"));
					ps.setString(4, thisChange.get("NEW_VALUE"));
					ps.setString(5, USER_ID);
					ps.setString(6, remoteAddr);
					if (ps.executeUpdate() > 0) {
						if (log.isDebugEnabled()) {
							log.debug("Person history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update person history for person " + id + "; " + e.getMessage(), e);
				eventService.logEvent(new Event(id, "Person history insert exception", "Exception encountered while attempting to update person history for person " + id + "; " + e.getMessage(), details, e));
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
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No changes detected for person " + id);
			}
		}
	}

	/**
	 * <p>Updates a person ID record using the data from the input parameters.</p>
	 *
	 * @param conn the database connection object
	 * @param id the ID of the person
	 * @param idType the type of id to save
	 * @param idValue the value of the id to save
	 * @return true if the insert was successful
	 */
	private boolean updatePersonID(Connection conn, String id, String idType, JSONObject thisId, String remoteAddr, JSONObject details) {
		boolean success = false;

		if (log.isDebugEnabled()) {
			log.debug("Updating ID of type \"" + idType + "\" with value of \"" + thisId.get("ID_VALUE") + "\" for person #" + id);
		}

		PreparedStatement ps = null;
		try {
			String sql = "UPDATE PERSON_ID SET IS_ACTIVE='Y', START_DATE=?, END_DATE=null, IS_PRIMARY=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='" + USER_ID + "', SYSMODTIME=getdate(), SYSMODADDR=? WHERE ID=?";
			if ("Y".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				if ("N".equalsIgnoreCase((String) thisId.get("ORIG_IS_ACTIVE"))) {
					sql = "UPDATE PERSON_ID SET IS_ACTIVE='Y', START_DATE=?, END_DATE=null, IS_PRIMARY=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='" + USER_ID + "', SYSMODTIME=getdate(), SYSMODADDR=? WHERE ID=?";
				}
			} else {
				sql = "UPDATE PERSON_ID SET IS_PRIMARY='N', IS_ACTIVE='N', START_DATE=?, END_DATE=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='" + USER_ID + "', SYSMODTIME=getdate(), SYSMODADDR=? WHERE ID=?";
			}
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			int i = 1;
			ps.setString(i++, getStartDate(thisId));
			if ("N".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				ps.setString(i++, getEndDate(thisId));
			} else {
				ps.setString(i++, (String) thisId.get("IS_PRIMARY"));
			}
			ps.setString(i++, remoteAddr);
			ps.setString(i++, (String) thisId.get("ID"));
			if (ps.executeUpdate() > 0) {
				success = true;
				if (log.isDebugEnabled()) {
					log.debug("ID successfully updated");
				}
				updatePersonIdHistory(id, thisId, remoteAddr, details);
			} else {
				log.error("Unable to update ID of type " + idType);
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to update ID of type " + idType + " for person " + id + "; " + e.getMessage(), e);
			eventService.logEvent(new Event(id, "Identity update error", "Exception encountered while attempting to update ID of type " + idType + " for person " + id + "; " + e.getMessage(), details, e));
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}

		return success;
	}

	/**
	 * <p>Inserts a person ID record using the data from the input parameters.</p>
	 *
	 * @param conn the database connection object
	 * @param id the ID of the person
	 * @param idType the type of id to save
	 * @param idValue the value of the id to save	 *
	 * @return true if the insert was successful
	 */
	private boolean insertPersonID(Connection conn, String id, String idType, JSONObject thisId, String remoteAddr, JSONObject details) {
		boolean success = false;

		if (log.isDebugEnabled()) {
			log.debug("Inserting  ID of type \"" + idType + "\" with value of \"" + thisId.get("ID_VALUE") + "\" for person #" + id);
		}

		PreparedStatement ps = null;
		try {
			String sql = "INSERT INTO PERSON_ID (PERSON_ID, ID_TYPE, ID_VALUE, IS_ACTIVE, START_DATE, IS_PRIMARY, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, 'Y', ?, ?, 0, '" + USER_ID + "', getdate(), ?)";
			if ("N".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				sql = "INSERT INTO PERSON_ID (PERSON_ID, ID_TYPE, ID_VALUE, IS_PRIMARY, IS_ACTIVE, START_DATE, END_DATE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, 'N', 'N', ?, ?, 0, '" + USER_ID + "', getdate(), ?)";
			}
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, id);
			ps.setString(2, idType);
			ps.setString(3, (String) thisId.get("ID_VALUE"));
			ps.setString(4, getStartDate(thisId));
			ps.setString(5, (String) thisId.get("IS_PRIMARY"));
			if ("N".equalsIgnoreCase((String) thisId.get("IS_ACTIVE"))) {
				ps.setString(5, getEndDate(thisId));
			}
			ps.setString(6, remoteAddr);
			if (ps.executeUpdate() > 0) {
				if (log.isDebugEnabled()) {
					log.debug("New ID successfully inserted");
				}
				success = true;
				updatePersonIdHistory(id, thisId, remoteAddr, details);
			} else {
				log.error("Unable to insert ID of type " + idType);
			}
		} catch (Exception e) {
			log.error("Exception encountered while attempting to insert  ID of type " + idType + " for person " + id + "; " + e.getMessage(), e);
			eventService.logEvent(new Event(id, "Person ID insert exception", "Exception encountered while attempting to insert  ID of type " + idType + " for person " + id + "; " + e.getMessage(), details, e));
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
		}

		return success;
	}

	private String getPersonStartDate(JSONObject newPerson, JSONObject oldPerson) {
		String startDate = getCurrentDate();

		if (StringUtils.isNotEmpty((String) newPerson.get("START_DATE"))) {
			startDate = (String) newPerson.get("START_DATE");
		} else if (oldPerson != null && StringUtils.isNotEmpty((String) oldPerson.get("START_DATE"))) {
			startDate = (String) oldPerson.get("START_DATE");
		} else if (StringUtils.isNotEmpty((String) newPerson.get("UC_PATH_START"))) {
			startDate = (String) newPerson.get("UC_PATH_START");
		} else if (StringUtils.isNotEmpty((String) newPerson.get("EXTERNAL_START"))) {
			startDate = (String) newPerson.get("EXTERNAL_START");
		}

		return startDate;
	}

	private String getStartDate(JSONObject thisId) {
		String startDate = getCurrentDate();

		if (StringUtils.isNotEmpty((String) thisId.get("START_DATE"))) {
			startDate = (String) thisId.get("START_DATE");
		} else if (StringUtils.isNotEmpty((String) thisId.get("ORIG_START_DATE"))) {
			startDate = (String) thisId.get("ORIG_START_DATE");
		}

		return startDate;
	}

	private String getEndDate(JSONObject thisId) {
		String endDate = getCurrentDate();

		if (StringUtils.isNotEmpty((String) thisId.get("END_DATE"))) {
			endDate = (String) thisId.get("END_DATE");
		}

		return endDate;
	}

	/**
	 * <p>Updates the person ID history for any column updated.</p>
	 *
	 * @param id the id of the person
	 * @param thisId the old and new values for the ID
	 */
	private void updatePersonIdHistory(String id, JSONObject thisId, String remoteAddr, JSONObject details) {
		if (log.isDebugEnabled()) {
			log.debug("Updating person history for person " + id + "; " + thisId.get("ID_TYPE") + ": " + thisId.get("ID_VALUE"));
		}

		// find changed columns
		List<Map<String,String>> change = new ArrayList<Map<String,String>>();
		for (int i=0; i<ID_FIELD.length; i++) {
			String thisFieldName = ID_FIELD[i];
			if (!isEqual2((String) thisId.get("ORIG_" + thisFieldName), (String) thisId.get(thisFieldName))) {
				Map<String,String> thisChange = new HashMap<String,String>();
				thisChange.put("COLUMN_NAME", thisFieldName);
				thisChange.put("OLD_VALUE", (String) thisId.get("ORIG_" + thisFieldName));
				thisChange.put("NEW_VALUE", (String) thisId.get(thisFieldName));
				change.add(thisChange);
			}
		}

		// update database
		if (change.size() > 0) {
			if (log.isDebugEnabled()) {
				log.debug(change.size() + " change(s) detected for person " + id + "; " + thisId.get("ID_TYPE") + ": " + thisId.get("ID_VALUE"));
			}
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = dataSource.getConnection();
				String sql = "INSERT INTO PERSON_HISTORY (PERSON_ID, ID_TYPE, ID_VALUE, COLUMN_NAME, OLD_VALUE, NEW_VALUE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES(?, ?, ?, ?, ?, ?, getdate(), ?, 0, ?)";
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + sql);
				}
				ps = conn.prepareStatement(sql);
				Iterator<Map<String,String>> i = change.iterator();
				while (i.hasNext()) {
					Map<String,String> thisChange = i.next();
					ps.setString(1, id);
					ps.setString(2, (String) thisId.get("ID_TYPE"));
					ps.setString(3, (String) thisId.get("ID_VALUE"));
					ps.setString(4, (String) thisChange.get("COLUMN_NAME"));
					ps.setString(5, (String) thisChange.get("OLD_VALUE"));
					ps.setString(6, (String) thisChange.get("NEW_VALUE"));
					ps.setString(7, USER_ID);
					ps.setString(8, remoteAddr);
					if (ps.executeUpdate() > 0) {
						if (log.isDebugEnabled()) {
							log.debug("Person history log updated: " + thisChange);
						}
					}
				}
			} catch (Exception e) {
				log.error("Exception encountered while attempting to update person history for person " + id + "; " + thisId.get("ID_TYPE") + ": " + thisId.get("ID_VALUE") + "; " + e.getMessage(), e);
				eventService.logEvent(new Event(id, "Person history insert exception", "Exception encountered while attempting to update person history for person " + id + "; " + thisId.get("ID_TYPE") + ": " + thisId.get("ID_VALUE") + "; " + e.getMessage(), details, e));
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
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No changes detected for person " + id + "; " + thisId.get("ID_TYPE") + ": " + thisId.get("ID_VALUE"));
			}
		}
	}

	/**
	 * <p>Fetches the external data from ServiceNow.</p>
	 *
	 * @param newPerson
	 * @param oldPerson
	 * @param details
	 * @return the updated person
	 */
	@SuppressWarnings("unchecked")
	private JSONObject addExternalData(JSONObject newPerson, JSONObject oldPerson, JSONObject details) {
		String id = (String) details.get("id");
		if (log.isDebugEnabled()) {
			log.debug("Fetching external data from ServiceNow for person " + id);
		}
		String externalId = (String) newPerson.get("EXTERNAL_ID");
		if (StringUtils.isEmpty(externalId)) {
			if (oldPerson != null) {
				externalId = (String) oldPerson.get("EXTERNAL_ID");
				newPerson.put("EXTERNAL_ID", externalId);
			}
		}
		String url = serviceNowServer + EXT_FETCH_URL + id;
		HttpGet get = new HttpGet(url);
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
				log.debug("HTTP response: " + resp);
			}
			if (rc == 200) {
				if (StringUtils.isNotEmpty(resp)) {
					JSONObject result = (JSONObject) JSONValue.parse(resp);
					JSONArray records = (JSONArray) result.get("result");
					if (records == null || records.size() == 0) {
						if (StringUtils.isNotEmpty(externalId)) {
							records = getExternalDataByExternalId(externalId, details);
						}
					}
					if (records != null && records.size() > 0) {
						JSONObject external = (JSONObject) records.get(0);
						if (log.isDebugEnabled()) {
							log.debug("External found for person " + id + ": " + external.toJSONString());
						}
						String extExternalId = (String) external.get("external_id");
						if (StringUtils.isEmpty(extExternalId)) {
							extExternalId = (String) external.get("number");
						}
						if (StringUtils.isEmpty(externalId) || !externalId.startsWith("H00")) {
							externalId = extExternalId;
							newPerson.put("EXTERNAL_ID", externalId);
						}
						if (StringUtils.isNotEmpty((String) external.get("title"))) {
							newPerson.put("TITLE", (String) external.get("title"));
						}
						if (StringUtils.isNotEmpty((String) external.get("supervisor.employee_number"))) {
							newPerson.put("SUPERVISOR", (String) external.get("supervisor.employee_number"));
						}
						if (StringUtils.isNotEmpty((String) external.get("department.u_id_6"))) {
							newPerson.put("HS_DEPT_ID", (String) external.get("department.u_id_6"));
						}
						if (StringUtils.isNotEmpty((String) external.get("phone"))) {
							newPerson.put("PHONE_NUMBER", fixTelephoneNumber((String) external.get("phone")));
						}
						if (StringUtils.isNotEmpty((String) external.get("location.u_location_code"))) {
							newPerson.put("LOCATION_CODE", (String) external.get("location.u_location_code"));
						}
						String email = (String) external.get("email");
						if (StringUtils.isNotEmpty(email)) {
							email = fixEmail(email);
							if (email.endsWith("ucdavis.edu")) {
								newPerson.put("EMAIL", email);
							} else {
								newPerson.put("ALTERNATE_EMAIL", email);
							}
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("External not found on ServiceNow for person " + id);
						}
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("External not found on ServiceNow for person " + id);
					}
				}
			} else {
				details.put("responseCode", rc + "");
				details.put("responseBody", resp);
				log.error("Invalid HTTP Response Code returned when fetching external data for person " + id + ": " + rc);
				eventService.logEvent(new Event(id, "Identity fetch error", "Invalid HTTP Response Code returned when fetching external data for person " + id + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for external for person " + id + ": " + e, e);
			eventService.logEvent(new Event(id, "Identity fetch exception", "Exception encountered searching for external for person " + id + ": " + e, details, e));
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning updated data: " + newPerson);
		}

		return newPerson;
	}

	/**
	 * <p>Fetches the external data from ServiceNow.</p>
	 *
	 * @param externalId
	 * @param details
	 * @return the array of ServiceNow External Identity records
	 */
	@SuppressWarnings("unchecked")
	private JSONArray getExternalDataByExternalId(String externalId, JSONObject details) {
		JSONArray records = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching external data from ServiceNow for External ID " + externalId);
		}
		String url = serviceNowServer + EXT_FETCH_URL2 + externalId;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
				log.debug("HTTP response: " + resp);
			}
			if (rc == 200) {
				if (StringUtils.isNotEmpty(resp)) {
					JSONObject result = (JSONObject) JSONValue.parse(resp);
					records = (JSONArray) result.get("result");
					if (records != null && records.size() > 0) {
						JSONObject external = (JSONObject) records.get(0);
						if (log.isDebugEnabled()) {
							log.debug("External found for External ID " + externalId + ": " + external.toJSONString());
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("External not found on ServiceNow for External ID " + externalId);
						}
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("External not found on ServiceNow for External ID " + externalId);
					}
				}
			} else {
				details.put("responseCode", rc + "");
				details.put("responseBody", resp);
				log.error("Invalid HTTP Response Code returned when fetching external data for External ID " + externalId + ": " + rc);
				eventService.logEvent(new Event(externalId, "Identity fetch error", "Invalid HTTP Response Code returned when fetching external data for External ID " + externalId + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for external with External ID " + externalId + ": " + e, e);
			eventService.logEvent(new Event(externalId, "Identity fetch exception", "Exception encountered searching for external with External ID " + externalId + ": " + e, details, e));
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning External data: " + records);
		}

		return records;
	}

	/**
	 * <p>Fetches the department manager from the Master database.</p>
	 *
	 * @param departmentId the ID of the department
	 * @return the department manager
	 */
	private String getDepartmentManager(String departmentId, JSONObject details) {
		String manager = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching department manager for departmentId " + departmentId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			String sql = "SELECT MANAGER FROM DEPARTMENT WHERE ALTERNATE_ID=?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, departmentId);
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Department " + departmentId + " found");
				}
				manager = rs.getString("MANAGER");
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Department " + departmentId + " not found");
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching Department " + departmentId + ": " + e.getMessage(), e);
			eventService.logEvent(new Event(departmentId, "Department Manager fetch exception", "Exception occurred when attempting to fetch Manager for Department " + departmentId + ": " + e, details, e));
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
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning manager: " + manager);
		}

		return manager;
	}

	/**
	 * <p>Fetches all of the IDs for the specified person.</p>
	 *
	 * @param id the ID of the person
	 * @return all of the IDs for the specified person
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchIds(String id, JSONObject details) {
		JSONObject ids = new JSONObject();
		details.put("ids", ids);

		if (log.isDebugEnabled()) {
			log.debug("Fetching all IDs for " + id);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			String sql = "SELECT ID, PERSON_ID, ID_TYPE, ID_VALUE, IS_PRIMARY, IS_ACTIVE, START_DATE, END_DATE FROM PERSON_ID WHERE PERSON_ID=?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, id);
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			rs = ps.executeQuery();
			int idCt = 0;
			while (rs.next()) {
				String idType = rs.getString("ID_TYPE");
				JSONArray idList = (JSONArray) ids.get(idType);
				if (idList == null) {
					idList = new JSONArray();
					ids.put(idType, idList);
				}
				JSONObject thisId = new JSONObject();
				thisId.put("ID", rs.getString("ID"));
				thisId.put("ID_TYPE", idType);
				thisId.put("ID_VALUE", rs.getString("ID_VALUE"));
				thisId.put("IS_PRIMARY", rs.getString("IS_PRIMARY"));
				thisId.put("ORIG_IS_PRIMARY", rs.getString("IS_PRIMARY"));
				thisId.put("IS_ACTIVE", rs.getString("IS_ACTIVE"));
				thisId.put("ORIG_IS_ACTIVE", rs.getString("IS_ACTIVE"));
				thisId.put("START_DATE", rs.getString("START_DATE"));
				thisId.put("ORIG_START_DATE", rs.getString("START_DATE"));
				thisId.put("END_DATE", rs.getString("END_DATE"));
				thisId.put("ORIG_END_DATE", rs.getString("END_DATE"));
				idList.add(thisId);
				idCt++;
			}
			if (log.isDebugEnabled()) {
				log.debug("Found " + idCt + " existing ID(s) for " + id);
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching IDs for " + id + ": " + e.getMessage(), e);
			eventService.logEvent(new Event(id, "Person IDs fetch exception", "Exception occurred when attempting to fetch all IDs for person " + id + ": " + e, details, e));
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
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning IDs: " + ids);
		}

		return ids;
	}

	/**
	 * <p>Verifies the specified ID.</p>
	 *
	 * @param idType the type of this ID
	 * @param idValue the value of this ID
	 * @return "Y" if the ID is active; "N" if it is not
	 */
	@SuppressWarnings("unchecked")
	private void verifyId(String idType, JSONObject thisId, JSONObject person, JSONObject details) {
		if (log.isDebugEnabled()) {
			log.debug("Verifying " + idType + " ID: " +  thisId.get("ID_VALUE"));
		}
		if ("HS A/D".equalsIgnoreCase(idType)) {
			verifyHsAdId(thisId, person, details);
		} else if ("KERBEROS".equalsIgnoreCase(idType)) {
			verifyKerberosId(thisId, person, details);
		} else if ("EXTERNAL".equalsIgnoreCase(idType) && ((String) thisId.get("ID_VALUE")).startsWith("H")) {
			verifyExternalId(thisId, person, details);
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Setting " + idType + " ID status to Active");
			}
			thisId.put("IS_ACTIVE", "Y");
		}
	}

	/**
	 * <p>Verifies the specified ID.</p>
	 *
	 * @param thisId the attributes of this ID
	 */
	@SuppressWarnings("unchecked")
	private void verifyHsAdId(JSONObject thisId, JSONObject person, JSONObject details) {
		thisId.put("IS_ACTIVE", "N");

		String url = "https://webtools.ucdmc.ucdavis.edu/hs/ldap/" + thisId.get("ID_VALUE");
		try {
			HttpClient client = HttpClientProvider.getClient();
			HttpGet get = new HttpGet(url);
			HttpResponse response = client.execute(get);
			if (response.getStatusLine().getStatusCode() == 200) {
				thisId.put("IS_ACTIVE", "Y");
			} else {
				if (response.getStatusLine().getStatusCode() == 404) {
					if (log.isDebugEnabled()) {
						log.debug("HS A/D account not found for " + thisId.get("ID_VALUE"));
					}
				} else {
					log.error("Invalid response code (" + response.getStatusLine().getStatusCode() + ") encountered accessing URL " + url);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + url, e);
			eventService.logEvent(new Event((String) thisId.get("ID_VALUE"), "LDAP URL exception", "Exception encountered accessing URL " + url, details, e));
		}
	}

	/**
	 * <p>Verifies the specified ID.</p>
	 *
	 * @param thisId the attributes of this ID
	 */
	@SuppressWarnings("unchecked")
	private void verifyKerberosId(JSONObject thisId, JSONObject person, JSONObject details) {
		thisId.put("IS_ACTIVE", "N");

		String url = "https://webtools.ucdmc.ucdavis.edu/hs/ldap2/" + thisId.get("ID_VALUE");
		try {
			HttpClient client = HttpClientProvider.getClient();
			HttpGet get = new HttpGet(url);
			HttpResponse response = client.execute(get);
			if (response.getStatusLine().getStatusCode() == 200) {
				thisId.put("IS_ACTIVE", "Y");
			} else {
				if (response.getStatusLine().getStatusCode() == 404) {
					if (log.isDebugEnabled()) {
						log.debug("Kerberos account not found for " + thisId.get("ID_VALUE"));
					}
				} else {
					log.error("Invalid response code (" + response.getStatusLine().getStatusCode() + ") encountered accessing URL " + url);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + url, e);
			eventService.logEvent(new Event((String) thisId.get("ID_VALUE"), "LDAP URL exception", "Exception encountered accessing URL " + url, details, e));
		}
	}

	/**
	 * <p>Verifies the specified ID.</p>
	 *
	 * @param thisId the attributes of this ID
	 */
	@SuppressWarnings("unchecked")
	private void verifyExternalId(JSONObject thisId, JSONObject person, JSONObject details) {
		thisId.put("IS_ACTIVE", "N");

		String idValue = (String) thisId.get("ID_VALUE");
		if (log.isDebugEnabled()) {
			log.debug("Verifying externalId " + idValue + " via ServiceNow");
		}
		String url = serviceNowServer + EXT_VERIFY_URL + idValue + "%5EORnumber%3D" + idValue;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching external data using url " + url);
			}
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
				log.debug("HTTP response: " + resp);
			}
			if (rc == 200) {
				if (StringUtils.isNotEmpty(resp)) {
					JSONObject result = (JSONObject) JSONValue.parse(resp);
					JSONArray records = (JSONArray) result.get("result");
					if (records != null && records.size() > 0) {
						JSONObject external = (JSONObject) records.get(0);
						if (log.isDebugEnabled()) {
							log.debug("External found for External ID " + idValue + ": " + external.toJSONString());
						}
						thisId.put("START_DATE", (String) external.get("start_date"));
						String endDate = (String) external.get("end_date");
						if (StringUtils.isNotEmpty(endDate) && endDate.compareTo(DATE_FORMAT.format(new Date())) < 0) {
							thisId.put("IS_ACTIVE", "N");
						} else {
							thisId.put("END_DATE", null);
							thisId.put("IS_ACTIVE", "Y");
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("External not found on ServiceNow for External ID " + idValue);
						}
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("External not found on ServiceNow for External ID " + idValue);
					}
				}
			} else {
				details.put("responseCode", rc + "");
				details.put("responseBody", resp);
				log.error("Invalid HTTP Response Code returned when fetching external data for external with External ID " + idValue + ": " + rc);
				eventService.logEvent(new Event(idValue, "External fetch error", "Invalid HTTP Response Code returned when fetching external data for external with External ID " + idValue + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for external with External ID " + idValue + ": " + e, e);
			eventService.logEvent(new Event(idValue, "External fetch exception", "Exception encountered searching for external with External ID " + idValue + ": " + e, details, e));
		}
	}

	/**
	 * <p>Adds/Updates the specified ID in the ID Map.</p>
	 *
	 * @param ids the Map of IDs
	 * @param idType the type of this ID
	 * @param idValue the value of this ID
	 * @return the updated map of IDs
	 */
	@SuppressWarnings("unchecked")
	private JSONObject addUpdateId(JSONObject ids, String idType, String idValue, JSONObject person, JSONObject details) {
		JSONArray idList = (JSONArray) ids.get(idType);
		if (idList == null) {
			idList = new JSONArray();
			ids.put(idType, idList);
		}
		JSONObject thisId = null;
		Iterator<JSONObject> i = idList.iterator();
		while (i.hasNext()) {
			JSONObject idData = i.next();
			if (idValue.equalsIgnoreCase((String) idData.get("ID_VALUE"))) {
				thisId = idData;
			}
		}
		if (thisId == null) {
			String startDate = getCurrentDate();
			String endDate = null;
			if ("UC PATH".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("UC_PATH_START"))) {
					startDate = (String) person.get("UC_PATH_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("UC_PATH_END"))) {
					endDate = (String) person.get("UC_PATH_END");
				}
			}
			if ("MOTHRA".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("MOTHRA_START"))) {
					startDate = (String) person.get("MOTHRA_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("MOTHRA_END"))) {
					endDate = (String) person.get("MOTHRA_END");
				}
			}
			if ("HS A/D".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("HS_AD_START"))) {
					startDate = (String) person.get("HS_AD_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("HS_AD_END"))) {
					endDate = (String) person.get("HS_AD_END");
				}
			}
			if ("KERBEROS".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("KERBEROS_START"))) {
					startDate = (String) person.get("KERBEROS_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("KERBEROS_END"))) {
					endDate = (String) person.get("KERBEROS_END");
				}
			}
			if ("PPS".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("PPS_START"))) {
					startDate = (String) person.get("PPS_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("PPS_END"))) {
					endDate = (String) person.get("PPS_END");
				}
			}
			if ("CAMPUS PPS".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("CAMPUS_PPS_START"))) {
					startDate = (String) person.get("CAMPUS_PPS_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("CAMPUS_PPS_END"))) {
					endDate = (String) person.get("CAMPUS_PPS_END");
				}
			}
			if ("EXTERNAL".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("EXTERNAL_START"))) {
					startDate = (String) person.get("EXTERNAL_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("EXTERNAL_END"))) {
					endDate = (String) person.get("EXTERNAL_END");
				}
			}
			if ("BANNER".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("BANNER_START"))) {
					startDate = (String) person.get("BANNER_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("BANNER_END"))) {
					endDate = (String) person.get("BANNER_END");
				}
			}
			if ("STUDENT".equals(idType)) {
				if (StringUtils.isNotEmpty((String) person.get("STUDENT_START"))) {
					startDate = (String) person.get("STUDENT_START");
				}
				if (StringUtils.isNotEmpty((String) person.get("STUDENT_END"))) {
					endDate = (String) person.get("STUDENT_END");
				}
			}
			thisId = new JSONObject();
			thisId.put("ID", "New");
			thisId.put("ID_TYPE", idType);
			thisId.put("ID_VALUE", idValue);
			thisId.put("IS_PRIMARY", "Y");
			thisId.put("ORIG_IS_PRIMARY", "Y");
			thisId.put("IS_ACTIVE", "Y");
			thisId.put("ORIG_IS_ACTIVE", "Y");
			thisId.put("START_DATE", startDate);
			thisId.put("ORIG_START_DATE", startDate);
			thisId.put("END_DATE", endDate);
			thisId.put("ORIG_END_DATE", endDate);
			idList.add(thisId);
		}
		thisId.put("IS_PRIMARY", "Y");
		thisId.put("IS_ACTIVE", "Y");
		thisId.put("IS_CURRENT", "Y");

		return ids;
	}

	/**
	 * <p>Builds a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the person data from the incoming request
	 */
	@SuppressWarnings("unchecked")
	private JSONObject buildPersonFromRequest(HttpServletRequest req, JSONObject details) {
		JSONObject person = new JSONObject();

		person.put("HS_AD_END", req.getParameter("adEnd"));
		person.put("HS_AD_ID", req.getParameter("adId"));
		person.put("HS_AD_START", req.getParameter("adStart"));
		person.put("ALTERNATE_EMAIL", fixEmail(req.getParameter("alternateEmail")));
		person.put("ALTERNATE_PHONES", req.getParameter("alternatePhones"));
		person.put("BANNER_END", req.getParameter("bannerEnd"));
		person.put("BANNER_ID", req.getParameter("bannerId"));
		person.put("BANNER_START", req.getParameter("bannerStart"));
		person.put("CAMPUS_PPS_END", req.getParameter("campusPpsEnd"));
		person.put("CAMPUS_PPS_ID", req.getParameter("campusPpsId"));
		person.put("CAMPUS_PPS_START", req.getParameter("campusPpsStart"));
		person.put("CELL_NUMBER", fixTelephoneNumber(req.getParameter("cellNumber")));
		person.put("DEPT_ID", req.getParameter("deptId"));
		person.put("EMAIL", fixEmail(req.getParameter("email")));
		person.put("END_DATE", req.getParameter("endDate"));
		person.put("EXTERNAL_END", req.getParameter("externalEnd"));
		person.put("EXTERNAL_ID", req.getParameter("externalId"));
		person.put("EXTERNAL_START", req.getParameter("externalStart"));
		person.put("FIRST_NAME", fixName(req.getParameter("firstName")));
		person.put("ID", req.getParameter("id"));
		person.put("IS_ACTIVE", req.getParameter("isActive"));
		person.put("IS_EMPLOYEE", req.getParameter("isEmployee"));
		person.put("IS_EXTERNAL", req.getParameter("isExternal"));
		person.put("IS_PREVIOUS_HS_EMPLOYEE", req.getParameter("isPrevEmployee"));
		person.put("IS_PREVIOUS_UCD_EMPLOYEE", req.getParameter("isPrevUcdEmployee"));
		person.put("IS_PREVIOUS_UCDH_EMPLOYEE", req.getParameter("isPrevUcdhEmployee"));
		person.put("IS_STUDENT", req.getParameter("isStudent"));
		person.put("IS_UCD_EMPLOYEE", req.getParameter("isUcdEmployee"));
		person.put("IS_UCDH_EMPLOYEE", req.getParameter("isUcdhEmployee"));
		person.put("KERBEROS_END", req.getParameter("kerberosEnd"));
		person.put("KERBEROS_ID", req.getParameter("kerberosId"));
		person.put("KERBEROS_START", req.getParameter("kerberosStart"));
		person.put("LAST_NAME", fixName(req.getParameter("lastName")));
		person.put("LOCATION_CODE", req.getParameter("locationCode"));
		person.put("MANAGER", req.getParameter("manager"));
		person.put("MERGED_INTO", req.getParameter("mergedInto"));
		person.put("MIDDLE_NAME", fixName(req.getParameter("middleName")));
		person.put("MOTHRA_END", req.getParameter("mothraEnd"));
		person.put("MOTHRA_ID", req.getParameter("mothraId"));
		person.put("MOTHRA_START", req.getParameter("mothraStart"));
		person.put("PAGER_NUMBER", fixTelephoneNumber(req.getParameter("pagerNumber")));
		person.put("PAGER_PROVIDER", req.getParameter("pagerProvider"));
		person.put("PHONE_NUMBER", fixTelephoneNumber(req.getParameter("phoneNumber")));
		person.put("PPS_END", req.getParameter("ppsEnd"));
		person.put("PPS_ID", req.getParameter("ppsId"));
		person.put("PPS_START", req.getParameter("ppsStart"));
		person.put("START_DATE", req.getParameter("startDate"));
		person.put("STUDENT_END", req.getParameter("studentEnd"));
		person.put("STUDENT_ID", req.getParameter("studentId"));
		person.put("STUDENT_MAJOR_NAME", req.getParameter("studentMajorName"));
		person.put("STUDENT_MAJOR", req.getParameter("studentMajor"));
		person.put("STUDENT_START", req.getParameter("studentStart"));
		person.put("SUPERVISOR", req.getParameter("supervisor"));
		person.put("TITLE", req.getParameter("title"));
		person.put("UC_PATH_END", req.getParameter("ucPathEnd"));
		person.put("UC_PATH_ID", req.getParameter("ucPathId"));
		person.put("UC_PATH_INSTITUTION", req.getParameter("ucPathInstitution"));
		person.put("UC_PATH_PERCENT", req.getParameter("ucPathPercent"));
		person.put("UC_PATH_REPRESENTATION", req.getParameter("ucPathRepresentation"));
		person.put("UC_PATH_START", req.getParameter("ucPathStart"));
		person.put("UC_PATH_TYPE", req.getParameter("ucPathType"));
		if (StringUtils.isNotEmpty((String) person.get("UC_PATH_ID"))) {
			if (StringUtils.isEmpty((String) person.get("UC_PATH_PERCENT")) || Float.parseFloat((String) person.get("UC_PATH_PERCENT")) == 0) {
				person.put("UC_PATH_PERCENT", "100.0");
			}
			if (StringUtils.isNotEmpty((String) person.get("PPS_ID"))) {
				if ("UCDH".equals(person.get("UC_PATH_INSTITUTION"))) {
					person.put("IS_UCDH_EMPLOYEE", "Y");
					person.put("IS_UCD_EMPLOYEE", "N");
					person.put("PPS_START", person.get("UC_PATH_START"));
					person.put("PPS_END", person.get("UC_PATH_END"));
				} else {
					person.put("IS_UCD_EMPLOYEE", "Y");
					person.put("IS_UCDH_EMPLOYEE", "N");
					person.put("CAMPUS_PPS_ID", person.get("PPS_ID"));
					person.put("CAMPUS_PPS_START", person.get("UC_PATH_START"));
					person.put("CAMPUS_PPS_END", person.get("UC_PATH_END"));
					person.put("PPS_ID", "");
					person.put("PPS_START", "");
					person.put("PPS_END", "");
				}
			}
		} else {
			person.put("IS_UCDH_EMPLOYEE", "N");
			person.put("IS_UCD_EMPLOYEE", "N");
		}
		if (StringUtils.isNotEmpty((String) person.get("EXTERNAL_ID")) && ((String) person.get("EXTERNAL_ID")).startsWith("H")) {
			person.put("IS_EXTERNAL", "Y");
		} else {
			person.put("IS_EXTERNAL", "N");
		}
		if (StringUtils.isNotEmpty((String) person.get("STUDENT_ID"))) {
			person.put("IS_STUDENT", "Y");
		} else {
			person.put("IS_STUDENT", "N");
		}
		if (!"UCDH".equals(person.get("UC_PATH_INSTITUTION"))) {
			person.put("DEPT_ID", "");
		}
		if ("delete".equalsIgnoreCase(req.getParameter("_action"))) {
			person.put("IS_ACTIVE", "N");
		}

		return person;
	}

	private String fixTelephoneNumber(String string) {
		String response = null;

		if (StringUtils.isNotEmpty(string)) {
			String justNumbers = string.trim().replaceAll("[^0-9]","");
			if (justNumbers.length() < 10) {
				int missingCharacters = 10 - justNumbers.length();
				justNumbers = "9167340000".substring(0, missingCharacters) + justNumbers;
				if (justNumbers.substring(0,6).equals("916733")) {
					justNumbers = "916703" + justNumbers.substring(6);
				}
			} else if (justNumbers.length() > 10) {
				justNumbers = justNumbers.substring(justNumbers.length() - 10);
			}
			response = "(";
			response += justNumbers.substring(0,3);
			response += ") ";
			response += justNumbers.substring(3,6);
			response += "-";
			response += justNumbers.substring(6,10);
			if (log.isDebugEnabled()) {
				log.debug("Phone number converted from \"" + string + "\" to \"" + response + "\".");
			}
		}

		return response;
	}

	private String fixEmail(String string) {
		String response = null;

		if (StringUtils.isNotEmpty(string)) {
			response = string.trim().toLowerCase();
			if (!string.equals(response)) {
				if (log.isDebugEnabled()) {
					log.debug("E-mail address converted from \"" + string + "\" to \"" + response + "\".");
				}
			}
		}

		return response;
	}

	private String fixName(String string) {
		String response = null;

		if (StringUtils.isNotEmpty(string)) {
			string = string.trim();
			if (string.length() > 1) {
				if (string.equals(string.toUpperCase()) || string.equals(string.toLowerCase())) {
					response = fixName1(string);
					if (log.isDebugEnabled()) {
						log.debug("Name converted from \"" + string + "\" to \"" + response + "\".");
					}
				} else {
					response = string;
				}
			} else {
				response = string.toUpperCase();
			}
		}

		return response;
	}

	private String fixName1(String string) {
		String response = "";

		if (string.indexOf(" ") != -1) {
			String separator = "";
			String[] parts = string.split(" ");
			for (int i=0; i<parts.length; i++) {
				response += separator;
				response += fixName2(parts[i]);
				separator = " ";
			}
		} else {
			response = fixName2(string);
		}

		return response;
	}

	private String fixName2(String string) {
		String response = "";

		if (StringUtils.isNotEmpty(string)) {
			if (string.length() > 1) {
				if (string.indexOf("-") != -1) {
					String separator = "";
					String[] parts = string.split("-");
					for (int i=0; i<parts.length; i++) {
						response += separator;
						response += fixName3(parts[i]);
						separator = "-";
					}
				} else {
					response = fixName3(string);
				}
			} else {
				response = string.toUpperCase();
			}
		}

		return response;
	}

	private String fixName3(String string) {
		String response = "";

		if (StringUtils.isNotEmpty(string)) {
			if (string.length() > 1) {
				if (string.indexOf("'") != -1) {
					String separator = "";
					String[] parts = string.split("'");
					for (int i=0; i<parts.length; i++) {
						response += separator;
						response += fixName4(parts[i]);
						separator = "'";
					}
				} else {
					response = fixName4(string);
				}
			} else {
				response = string.toUpperCase();
			}
		}

		return response;
	}

	private String fixName4(String string) {
		String response = "";

		if (StringUtils.isNotEmpty(string)) {
			if (string.length() > 1) {
				string = string.toLowerCase();
				response = string.substring(0, 1).toUpperCase() + string.substring(1);
			} else {
				response = string.toUpperCase();
			}
		}

		return response;
	}

	private boolean isEqual(JSONObject p1, JSONObject p2, String key) {
		boolean eq = true;

		String s1 = (String) p1.get(key);
		String s2 = (String) p2.get(key);
		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
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

	private boolean isEqual2(String s1, String s2) {
		boolean eq = true;

		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
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

	/**
	 * <p>Publishes the current values for the specified ID.</p>
	 *
	 * @param id the person id
	 * @param action the update action (add, change, or delete)
	 */
	private void publishUpdate(String id, String action, JSONObject details) {
		if (log.isDebugEnabled()) {
			log.debug("Publishing " + action + " action for person #" + id);
		}

		if (field.size() == 0) {
			establsihFieldInformation();
		}
		if (field.size() > 0) {
			Connection conn = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				conn = dataSource.getConnection();
				ps = conn.prepareStatement(FETCH_SQL);
				ps.setString(1, id);
				if (log.isDebugEnabled()) {
					log.debug("Using the following SQL: " + FETCH_SQL);
				}
				rs = ps.executeQuery();
				if (rs.next()) {
					if (log.isDebugEnabled()) {
						log.debug("Person #" + id + " found");
					}
					Properties properties = new Properties();
					Iterator<Map<String,String>> i = field.iterator();
					while (i.hasNext()) {
						Map<String,String> thisField = i.next();
						String fieldName = thisField.get("fieldName");
						String columnName = thisField.get("columnName");
						String fieldValue = rs.getString(columnName);
						if (StringUtils.isEmpty(fieldValue)) {
							fieldValue = "";
						}
						properties.setProperty(fieldName, fieldValue);
					}
					up2dateService.post(new Update(publisherId, action, properties));
				}
			} catch (SQLException e) {
				log.error("Exception encountered fetching data to publish for person #" + id + ": " + e.getMessage(), e);
				eventService.logEvent(new Event(id, "VIEW_PERSON_ALL fetch exception", "Exception encountered fetching data to publish for person #" + id + ": " + e.getMessage(), details, e));
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
				if (conn != null) {
					try {
						conn.close();
					} catch (Exception e) {
						//
					}
				}
			}
		} else {
			log.error("Unable to publish person #" + id + "; no field information available from publisher.");
			eventService.logEvent(new Event(id, "Publisher informmation fetch exception", "Unable to publish person #" + id + "; no field information available from publisher.", details));
		}
	}

	private void establsihFieldInformation() {
		Properties publisher = up2dateService.getPublisher(publisherId);
		if (publisher != null) {
			String[] fieldList = publisher.getProperty("field").split(";");
			if (fieldList.length > 0) {
				for (int i=0; i<fieldList.length; i++) {
					Map<String,String> thisField = new HashMap<String,String>();
					String[] parts = fieldList[i].split(",");
					thisField.put("fieldName", parts[0]);
					thisField.put("columnName", parts[1]);
					field.add(thisField);
				}
				log.info("Publisher data obtained for publisher " + publisherId + "; data fields: " + field);
			}
		}
	}

	private static String getCurrentDate() {
		return formatDate(new Date());
	}

	private static String formatDate(Date dt) {
		String response = null;

		if (dt != null) {
			response = DATE_FORMAT.format(dt) + " 00:00:00.0";
		}

		return response;
	}
}