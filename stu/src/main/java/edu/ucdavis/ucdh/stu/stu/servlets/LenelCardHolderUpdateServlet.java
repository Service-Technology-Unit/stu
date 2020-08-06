package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.snutil.util.EventService;

/**
 * <p>This servlet updates the CardKey cardholder database with data from the UCDH Person Repository.</p>
 */
public class LenelCardHolderUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private static final String AUTH_URL = "/api/access/onguard/openaccess/authentication?version=1.0";
	private static final String FETCH_URL = "/api/access/onguard/openaccess/instances?version=1.0&type_name=Lnl_Cardholder&filter=IAMID%3D";
	private static final String UPDATE_URL = "/api/access/onguard/openaccess/instances?version=1.0";
	private static final String GRANT_FETCH_URL = "/api/now/table/x_ucdhs_access_gra_access_grant?sysparm_fields=status&sysparm_query=grant.nameLIKEbadge%5Estatus%3Dactive%5Euser.employee_number%3D";
	private Log log = LogFactory.getLog(getClass());
	private int iamIdIndex = -1;
	private boolean includeExternals = false;
	private String authorizedIpList = null;
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;
	private String lenelServer = null;
	private String lenelUser = null;
	private String lenelPassword = null;
	private String lenelDirectory = null;
	private String lenelApplicationId = null;
	private EventService eventService = null;
	private List<String> authorizedIp = new ArrayList<String>();
	private DataSource dataSource = null;
	private DataSource prDataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		Boolean incExternals = (Boolean) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("cardKeyIncludeExternals");
		if (incExternals) {
			includeExternals = true;
		}
		authorizedIpList = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("authorizedIpList");
		if (StringUtils.isNotEmpty(authorizedIpList)) {
			String[] ipArray = authorizedIpList.split(",");
			for (int i=0; i<ipArray.length; i++) {
				authorizedIp.add(ipArray[i].trim());
			}
		}
		serviceNowServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowServer");
		serviceNowUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowUser");
		serviceNowPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowPassword");
		lenelServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("lenelServer");
		lenelUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("lenelUser");
		lenelPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("lenelPassword");
		lenelDirectory = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("lenelDirectory");
		lenelApplicationId = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("lenelApplicationId");
		eventService = (EventService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("eventService");
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("cardKeyDataSource");
		prDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("masterDataSource");
	}

	/**
	 * <p>The Servlet "GET" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The GET method is not allowed for this URL", null);
    }

	/**
	 * <p>The Servlet "doPost" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";
		String requestId = req.getParameter("_rid");
		String subscriptionId = req.getParameter("_sid");
		String publisherId = req.getParameter("_pid");
		String action = req.getParameter("_action");
		String id = req.getParameter("id");

		if (log.isDebugEnabled()) {
			log.debug("Processing new update - Publisher: " + publisherId + "; Subscription: " + subscriptionId + "; Request: " + requestId + "; IAM ID: " + id);
		}

		JSONObject details = new JSONObject();
		details.put("requestId", requestId);
		details.put("subscriptionId", subscriptionId);
		details.put("publisherId", publisherId);
		details.put("action", action);
		details.put("id", id);

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		if (authorizedIp.contains(remoteAddr)) {
			if (StringUtils.isNotEmpty(action)) {
				if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete") || action.equalsIgnoreCase("force")) {
					if (StringUtils.isNotEmpty(id)) {
						details.put("sessionToken", getSessionToken(details));
						JSONObject person = buildPersonFromRequest(req, details);
						details.put("person", person);
						if (StringUtils.isEmpty((String) person.get("bypassReason"))) {
							Map<String,String> oldPerson = fetchCardholder(id, (String) person.get("UNIQUE_KEY"), details);
							if (oldPerson != null) {
								details.put("oldPerson", oldPerson);
								if (!action.equalsIgnoreCase("force") && personUnchanged(req, person, oldPerson)) {
									response = "1;No action taken -- no changes detected";
								} else {
									response = updateCardholder(req, res, person, oldPerson, details);
								}
							} else {
								if (action.equalsIgnoreCase("delete")) {
									response = "1;No action taken -- person not on file";
								} else {
									if ("INACTIVE".equalsIgnoreCase((String) person.get("HR_NOTES"))) {
										response = "1;No action taken -- INACTIVE persons are not inserted";
									} else {
										response = updateCardholder(req, res, person, null, details);
									}
								}
							}
						} else {
							response = "1;No action taken -- " + person.get("bypassReason");
						}
					} else {
						response = "2;Error - Required parameter \"id\" has no value";
					}
				} else {
					response = "2;Error - Invalid action: " + action;
				}
			} else {
				response = "2;Error - Required parameter \"_action\" has no value";
			}
			if (log.isDebugEnabled()) {
				log.debug("Response: " + response);
			}
			res.setCharacterEncoding("UTF-8");
			res.setContentType("text/plain;charset=UTF-8");
			res.getWriter().write(response);
		} else {
			sendError(req, res, HttpServletResponse.SC_FORBIDDEN, remoteAddr + " is not authorized to access this service", details);
		}
	}

	/**
	 * <p>Returns the Lenel session token provided by authenticating to the Lenel REST API.</p>
	 *
	 * @param details the JSON object containing the details of this transaction
	 * @return the Lenel session token
	 */
	@SuppressWarnings("unchecked")
	private String getSessionToken(JSONObject details) {
		String token = null;

		if (log.isDebugEnabled()) {
			log.debug("Authenticating to Lenel to obtain a valid session token");
		}

		String url = lenelServer + AUTH_URL;
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
		post.setHeader("Application-Id", lenelApplicationId);
		JSONObject postData = new JSONObject();
		postData.put("user_name", lenelUser);
		postData.put("password", lenelPassword);
		postData.put("directory_id", lenelDirectory);
		try {
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Authenticating to Lenel to obtain a valid session token using url " + url);
			}
			post.setEntity(new StringEntity(postData.toJSONString(), "utf-8"));
			HttpResponse response = client.execute(post);
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc == 200) {
				token = (String) result.get("session_token");
				if (StringUtils.isNotEmpty(token)) {
					if (log.isDebugEnabled()) {
						log.debug("Lenel Session Token: " + token);
					}
				} else {
					log.error("Unable to obtain a valid session token when authenticating to Lenel.");
					eventService.logEvent(new Event((String) details.get("id"), "Session token fetch error", "Unable to obtain a valid session token when authenticating to Lenel.", details));
				}
			} else {
				log.error("Invalid HTTP Response Code returned when authenticating to Lenel to obtain a valid session token: " + rc);
				eventService.logEvent(new Event((String) details.get("id"), "Session token fetch error", "Invalid HTTP Response Code returned when authenticating to Lenel to obtain a valid session token: " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered when authenticating to Lenel to obtain a valid session token: " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Session token fetch exception", "Exception encountered when authenticating to Lenel to obtain a valid session token: " + e, details, e));
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning Lenel Session Token: " + token);
		}

		return token;
	}

	/**
	 * <p>Returns the card holder data on file in the card holder database, if present.</p>
	 *
	 * @param id the IAM ID of the person
	 * @param ucPathId the UCPath ID of the person
	 * @param details the JSON object containing the details of this transaction
	 * @return the cardholder's data from the card key system
	 */
	private JSONObject fetchCardholder(String id, String key, JSONObject details) {
		JSONObject person = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching card key database for IAM ID #" + id + " ...");
		}
		String url = lenelServer + FETCH_URL + id;
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		get.setHeader("Session-Token", (String) details.get("sessionToken"));
		get.setHeader("Application-Id", lenelApplicationId);
		try {
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching cardholder data using url " + url);
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc == 200) {
				JSONArray records = (JSONArray) result.get("item_list");
				if (records != null && records.size() > 0) {
					person = (JSONObject) ((JSONObject) records.get(0)).get("property_value_map");
					if (log.isDebugEnabled()) {
						log.debug("Cardholder found for IAM ID " + id + ": " + person.toJSONString());
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Cardholder not found on lenel for IAM ID " + id);
					}
				}
			} else {
				log.error("Invalid HTTP Response Code returned when fetching Cardholder data for IAM ID " + id + ": " + rc);
				eventService.logEvent(new Event(id, "Cardholder fetch error", "Invalid HTTP Response Code returned when fetching Cardholder data for IAM ID " + id + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered when fetching Cardholder data for IAM ID " + id + ": " + e, e);
			eventService.logEvent(new Event(id, "Cardholder fetch exception", "Exception encountered when fetching Cardholder data for IAM ID " + id + ": " + e, details, e));
		}
		if (log.isDebugEnabled() && person != null) {
			log.debug("Returning existing cardholder: " + person.toJSONString());
		}

		return person;
	}

	/**
	 * <p>Inserts a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param person the original person data
	 * @return true if the person is unchanged
	 */
	private boolean personUnchanged(HttpServletRequest req, Map<String,String> person, Map<String,String> oldPerson) {
		boolean unchanged = false;

		if (isEqual(oldPerson, person, "ADDRESS") &&
				isEqual(oldPerson, person, "ALT ID") &&
				isEqual(oldPerson, person, "CITY") &&
				isEqual(oldPerson, person, "EMAIL") &&
				isEqual(oldPerson, person, "EXPIRATION_DATE") &&
				isEqual(oldPerson, person, "FIRST_NAME") &&
				isEqual(oldPerson, person, "HR_DEPT") &&
				isEqual(oldPerson, person, "HR_DEPTID") &&
				isEqual(oldPerson, person, "HR_TITLE") &&
				isEqual(oldPerson, person, "IAM ID") &&
				isEqual(oldPerson, person, "LAST_NAME") &&
				isEqual(oldPerson, person, "MIDDLE_NAME") &&
				isEqual(oldPerson, person, "ROOM") &&
				isEqual(oldPerson, person, "TITLE 1") &&
				isEqual(oldPerson, person, "TITLE 2") &&
				isEqual(oldPerson, person, "TITLE 3") &&
				isEqual(oldPerson, person, "UC_PATH_ID") &&
				isEqual(oldPerson, person, "ZIP")) {
			unchanged = true;
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param person the new data for this person
	 * @param oldPerson the existing data for this person
	 * @return the response
	 */
	private String updateCardholder(HttpServletRequest req, HttpServletResponse res, Map<String,String> person, Map<String,String> oldPerson, JSONObject details) {
		String response = null;

		String cardholderId = null;
		if (oldPerson != null) {
			if (StringUtils.isNotEmpty(oldPerson.get("CARDHOLDER_ID"))) {
				cardholderId = oldPerson.get("CARDHOLDER_ID");
			}
			if (StringUtils.isEmpty(person.get("HR_DEPTID"))) {
				person.put("HR_DEPT", oldPerson.get("HR_DEPT"));
				person.put("HR_DEPTID", oldPerson.get("HR_DEPTID"));
			}
		}
		if (log.isDebugEnabled()) {
			if (StringUtils.isNotEmpty(cardholderId)) {
				log.debug("Updating existing cardholder data for " + person.get("FIRST_NAME") + " " + person.get("LAST_NAME") + " (" + cardholderId + ")");
			} else {
				log.debug("Inserting data for new cardholder " + person.get("FIRST_NAME") + " " + person.get("LAST_NAME"));
			}
		}
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement("INSERT INTO Pegasys_Util.dbo.Super_User_Queue ([CardHolderID], [RequestID], [FirstName], [MiddleName], [LastName], [NickName], [Address], [City], [State], [Zip], [Room], [Phone], [Email], [StartDate], [EndDate], [SponsorID], [NAME], [FIRST], [LAST], [DEGREES], [TITLE_1], [TITLE_2], [TITLE_3], [EXPIRATION_DATE], [EMP_ID], [ALT_ID], [MEAL_CARD], [NOTES_1], [NOTES_2], [HR_TITLE], [HR_DEPT], [HR_DEPTID], [HR_NOTES], [IAM_ID], [QueueStart]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate())");
			ps.setString(1, nullify(cardholderId));
			ps.setString(2, nullify(req.getParameter("_rid")));
			ps.setString(3, nullify(person.get("FIRST_NAME")));
			ps.setString(4, nullify(person.get("MIDDLE_NAME")));
			ps.setString(5, nullify(person.get("LAST_NAME")));
			ps.setString(6, nullify(person.get("UNIQUE_KEY")));
			ps.setString(7, nullify(person.get("ADDRESS")));
			ps.setString(8, nullify(person.get("CITY")));
			ps.setString(9, nullify(person.get("STATE")));
			ps.setString(10, nullify(person.get("ZIP")));
			ps.setString(11, nullify(person.get("ROOM")));
			ps.setString(12, nullify(person.get("PHONE")));
			String email = null;
			if (StringUtils.isNotEmpty(person.get("EMAIL"))) {
				email = person.get("EMAIL").toLowerCase();
			}
			ps.setString(13, email);
			ps.setString(14, nullify(person.get("VALID_FROM")));
			ps.setString(15, nullify(person.get("VALID_TO")));
			ps.setString(16, nullify(person.get("SPONSOR")));
			ps.setString(17, nullify(person.get("NAME")));
			ps.setString(18, nullify(person.get("FIRST")));
			ps.setString(19, nullify(person.get("LAST")));
			ps.setString(20, nullify(person.get("DEGREES")));
			ps.setString(21, nullify(person.get("TITLE_1")));
			ps.setString(22, nullify(person.get("TITLE_2")));
			ps.setString(23, nullify(person.get("TITLE_3")));
			ps.setString(24, nullify(person.get("EXPIRATION_DATE")));
			ps.setString(25, nullify(person.get("EMP_ID")));
			ps.setString(26, nullify(person.get("ALT_ID")));
			ps.setString(27, nullify(person.get("MEAL_CARD")));
			ps.setString(28, nullify(person.get("NOTES_1")));
			ps.setString(29, nullify(person.get("NOTES_2")));
			ps.setString(30, nullify(person.get("HR_TITLE")));
			ps.setString(31, nullify(person.get("HR_DEPT")));
			ps.setString(32, nullify(person.get("HR_DEPTID")));
			ps.setString(33, nullify(person.get("HR_NOTES")));
			ps.setString(34, nullify(person.get("IAM_ID")));
			int insertCt = ps.executeUpdate();
			if (insertCt == 1) {
				response = "0;Update added to the pending work queue";
			} else {
				response = "1;Unable to add update to the pending work queue";
				log.error("Unable to add update to the pending work queue");
				eventService.logEvent(new Event(person.get("IAM_ID"), "Card Holder update error", "Unable to add update to the pending work queue", details));
			}
		} catch (Exception e) {
			response = "2;Exception occurred while attempting to add to the pending work queue: " + e;
			log.error("Exception occurred while attempting to add to the pending work queue: " + e, e);
			eventService.logEvent(new Event(person.get("IAM_ID"), "Card Holder update exception", "Exception occurred while attempting to add to the pending work queue: " + e, details, e));
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Update response: " + response);
		}

		return response;
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

		person.put("FIRST_NAME", req.getParameter("firstName"));
		person.put("MIDDLE_NAME", req.getParameter("middleName"));
		person.put("LAST_NAME", req.getParameter("lastName"));
		person.put("UC_PATH_ID", req.getParameter("ucPathId"));
		person.put("ADDRESS", req.getParameter("address"));
		person.put("CITY", req.getParameter("city"));
		person.put("STATE", req.getParameter("state"));
		person.put("ZIP", req.getParameter("zip"));
		person.put("ROOM", req.getParameter("room"));
		person.put("PHONE", req.getParameter("phoneNumber"));
		person.put("EMAIL", req.getParameter("email"));
		person.put("VALID_FROM", req.getParameter("startDate"));
		person.put("VALID_TO", req.getParameter("endDate"));
		person.put("NAME", req.getParameter("firstName") + " " + req.getParameter("lastName"));
		person.put("FIRST", req.getParameter("firstName"));
		person.put("LAST", req.getParameter("lastName"));
		if (StringUtils.isNotEmpty(req.getParameter("endDate"))) {
			person.put("EXPIRATION_DATE", "Exp: " + req.getParameter("endDate").substring(0, 10));
		}
		if (StringUtils.isNotEmpty(req.getParameter("ucPathId"))) {
			if ("UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution"))) {
				person.put("EMP_ID", req.getParameter("ucPathId"));
			} else {
				person.put("ALT_ID", "CMP:" + req.getParameter("ucPathId"));
			}
		}
		if (StringUtils.isEmpty((String) person.get("ALT_ID"))) {
			if (StringUtils.isNotEmpty(req.getParameter("studentId"))) {
				person.put("ALT_ID", "STU:" + req.getParameter("studentId"));
			} else if (StringUtils.isNotEmpty(req.getParameter("externalId")) && req.getParameter("externalId").startsWith("H0")) {
				person.put("ALT_ID", "EXT:" + req.getParameter("externalId"));
			} else if (StringUtils.isEmpty((String) person.get("EMP_ID"))) {
				person.put("ALT_ID", "IAM:" + req.getParameter("id"));
			}
		}
		if (StringUtils.isNotEmpty((String) person.get("EMP_ID"))) {
			person.put("UNIQUE_KEY", (String) person.get("EMP_ID"));
		} else {
			person.put("UNIQUE_KEY", (String) person.get("ALT_ID"));
		}
		if (StringUtils.isNotEmpty(req.getParameter("title"))) {
			String title = req.getParameter("title").trim();
			if (title.length() > 64) {
				if (log.isDebugEnabled()) {
					log.debug("Truncating Title to first 64 characters: " + title);
				}
				title = title.substring(0, 64);
			}
			person.put("HR_TITLE", title);
		}
		person.put("HR_DEPT", req.getParameter("deptName"));
		person.put("HR_DEPTID", req.getParameter("deptId"));
		if (!"Y".equalsIgnoreCase(req.getParameter("isActive"))) {
			person.put("HR_NOTES", "INACTIVE");
		}
		person.put("IAM_ID", req.getParameter("id"));
		if (StringUtils.isEmpty(req.getParameter("ucPathId")) || !"UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution"))) {
			person = processExternal(req, person, details);
		} else {
			person.put("SPONSOR", null);			
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning new cardholder values: " + person);
		}

		return person;
	}

	/**
	 * <p>Augments the new person object with data related to Externals</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param person the current person object
	 * @return the augmented person object
	 */
	@SuppressWarnings("unchecked")
	private JSONObject processExternal(HttpServletRequest req, JSONObject person, JSONObject details) {
		if (includeExternals) {
			if (activeBadgeGrant((String) person.get("IAM_ID"), details)) {
				person.put("SPONSOR", getUcpathId(req.getParameter("supervisor"), details));
				if (StringUtils.isEmpty((String) person.get("SPONSOR"))) {
					person.put("SPONSOR", getUcpathId(req.getParameter("manager"), details));
					if (StringUtils.isEmpty((String) person.get("SPONSOR"))) {
						if (StringUtils.isNotEmpty((String) person.get("HR_DEPTID"))) {
							person.put("SPONSOR", getUcpathId(getDepartmentManager((String) person.get("HR_DEPTID"), details), details));
						}
					}
				}
			} else {
				person.put("bypassReason", "Externals without an active Badge Grant are not included");
			}
		} else {
			person.put("bypassReason", "Externals are not included at this time");
		}

		return person;
	}


	/**
	 * <p>Looks for an active Badge Grant for the specified External</p>
	 *
	 * @param id the IAM ID of the person
	 * @param details the JSON object containing the details of this transaction
	 * @return true if the External has an active Badge Grant
	 */
	private boolean activeBadgeGrant(String id, JSONObject details) {
		boolean activeGrant = false;

		if (log.isDebugEnabled()) {
			log.debug("Checking for Badge Access Grant for " + id + " via ServiceNow");
		}
		String url = serviceNowServer + GRANT_FETCH_URL + id;
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc == 200) {
				JSONArray records = (JSONArray) result.get("result");
				if (records != null && records.size() > 0) {
					if (log.isDebugEnabled()) {
						log.debug("Badge Access Grant found for IAM ID " + id);
					}
					activeGrant = true;
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Badge Access Grant not found on ServiceNow for IAM ID " + id);
					}
				}
			} else {
				log.error("Invalid HTTP Response Code returned when fetching Access Grant data for IAM ID " + id + ": " + rc);
				eventService.logEvent(new Event(id, "Access Grant fetch error", "Invalid HTTP Response Code returned when fetching Access Grant data for IAM ID " + id + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered when fetching Access Grant data for IAM ID " + id + ": " + e, e);
			eventService.logEvent(new Event(id, "Access Grant fetch exception", "Exception encountered when fetching Access Grant data for IAM ID " + id + ": " + e, details, e));
		}

		return activeGrant;
	}

	/**
	 * <p>Returns the UC Path ID for the IAM ID passed.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @return the UC Path ID for the IAM ID passed
	 */
	private String getUcpathId(String iamId, JSONObject details) {
		String ucPathId = null;

		if (StringUtils.isNotEmpty(iamId)) {
			if (log.isDebugEnabled()) {
				log.debug("Searching for UCPath ID for IAM ID #" + iamId + " ...");
			}
			Connection con = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				con = dataSource.getConnection();
				ps = con.prepareStatement("SELECT b.c_nick_name FROM udftext a LEFT OUTER JOIN cardholder b ON b.c_id=a.ut_cardholder_id WHERE a.ut_udfgen_id=" + iamIdIndex + " AND a.ut_text=?");
				ps.setString(1, iamId);
				rs = ps.executeQuery();
				if (rs.next()) {
					ucPathId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("Found IAM ID #" + iamId + " on file with a UCPath ID of " + ucPathId);
					}
				}
			} catch (Exception e) {
				log.error("Exception occurred while attempting to find UCPath ID for IAM ID #" + iamId + ": " + e, e);
				eventService.logEvent(new Event(iamId, "UC Path ID fetch exception", "Exception occurred while attempting to find UCPath ID for IAM ID #" + iamId + ": " + e, details, e));
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No UCPATH ID available for empty IAM ID");
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning ucPathId: " + ucPathId);
		}

		return ucPathId;
	}

	/**
	 * <p>Returns the IAM ID for the manager of the specified department.</p>
	 *
	 * @param deptId the ID of the department
	 * @return the IAM ID for the manager of the specified department
	 */
	private String getDepartmentManager(String deptId, JSONObject details) {
		String iamId = null;

		if (StringUtils.isNotEmpty(deptId)) {
			if (log.isDebugEnabled()) {
				log.debug("Searching for the IAM ID of the Manager for Department ID " + deptId + " ...");
			}
			Connection con = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				con = prDataSource.getConnection();
				ps = con.prepareStatement("SELECT MANAGER FROM DEPARTMENT WHERE ALTERNATE_ID=?");
				ps.setString(1, deptId);
				rs = ps.executeQuery();
				if (rs.next()) {
					iamId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("Found Department ID " + deptId + " on file with a Manager IAM ID of " + iamId);
					}
				}
			} catch (Exception e) {
				log.error("Exception occurred while attempting to find the IAM ID of the Manager for Department ID " + deptId + ": " + e, e);
				eventService.logEvent(new Event(iamId, "Dept Manager fetch exception", "Exception occurred while attempting to find the IAM ID of the Manager for Department ID " + deptId + ": " + e, details, e));
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Unable to search for Department Manager for blank Dept ID");
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning Department Manager IAM ID: " + iamId);
		}

		return iamId;
	}

	private boolean isEqual(Map<String,String> p1, Map<String,String> p2, String key) {
		boolean eq = true;

		String s1 = p1.get(key);
		String s2 = p2.get(key);
		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
			}
		} else {
			if (StringUtils.isNotEmpty(s2)) {
				eq = false;
			}
		}

		return eq;
	}

	private String nullify(String value) {
		String response = null;

		if (StringUtils.isNotEmpty(value)) {
			response = value.trim();
			if ("null".equalsIgnoreCase(response)) {
				response = null;
			}
		}

		return response;
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details) throws IOException {
		sendError(req, res, errorCode, errorMessage, details, null);
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 * @param throwable an optional exception
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, JSONObject details, Throwable throwable) throws IOException {
		// log message
		if (throwable != null) {
			log.error("Sending error " + errorCode + "; message=" + errorMessage, throwable);
		} else if (log.isDebugEnabled()) {
			log.debug("Sending error " + errorCode + "; message=" + errorMessage);
		}

		// verify details
		if (details == null) {
			details = new JSONObject();
		}

		// log event
		eventService.logEvent(new Event((String) details.get("id"), "HTTP response", "Sending error " + errorCode + "; message=" + errorMessage, details, throwable));

		// send error
		res.setContentType("text/plain;charset=UTF-8");
		res.sendError(errorCode, errorMessage);
	}
}
