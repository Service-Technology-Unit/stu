package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
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
import org.apache.http.client.methods.HttpPut;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.snutil.util.EventService;

/**
 * <p>This servlet updates the Lenel OnGuard cardholder database with data from the UCDH Person Repository.</p>
 */
public class LenelCardHolderUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private static final String AUTH_URL = "/api/access/onguard/openaccess/authentication?version=1.0";
	private static final String FETCH_URL = "/api/access/onguard/openaccess/instances?version=1.0&type_name=Lnl_Cardholder&filter=IAM_ID%3D%22";
	private static final String FETCH_URL2 = "/api/access/onguard/openaccess/instances?version=1.0&type_name=Lnl_Cardholder&filter=SSNO%3D%22";
	private static final String DEPT_FETCH_URL = "/api/access/onguard/openaccess/instances?version=1.0&type_name=Lnl_DEPT&filter=Name%20like%20%22";
	private static final String TITLE_FETCH_URL = "/api/access/onguard/openaccess/instances?version=1.0&type_name=Lnl_TITLE&filter=Name%3D%22";
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
							JSONObject oldPerson = fetchCardholder(id, details);
							if (oldPerson != null) {
								details.put("oldPerson", oldPerson);
								if (!action.equalsIgnoreCase("force") && personUnchanged(req, person, oldPerson)) {
									response = "1;No action taken -- no changes detected";
								} else {
									response = updateCardholder(person, oldPerson, details);
								}
							} else {
								if (action.equalsIgnoreCase("delete")) {
									response = "1;No action taken -- person not on file";
								} else {
									if ("INACTIVE".equalsIgnoreCase((String) person.get("HR_NOTES"))) {
										response = "1;No action taken -- INACTIVE persons are not inserted";
									} else {
										response = insertCardholder(person, details);
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
		JSONObject tokenRequest = new JSONObject();
		JSONObject tokenResponse = new JSONObject();
		details.put("tokenRequest", tokenRequest);
		details.put("tokenResponse", tokenResponse);
		tokenRequest.put("url", url);
		tokenRequest.put("tokenData", postData);
		try {
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Authenticating to Lenel to obtain a valid session token using url " + url);
			}
			post.setEntity(new StringEntity(postData.toJSONString(), "utf-8"));
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			tokenResponse.put("responseCode", rc);
			tokenResponse.put("responseString", jsonRespString);
			tokenResponse.put("responseObject", result);
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
	 * @param details the JSON object containing the details of this transaction
	 * @return the cardholder's data from the card key system
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchCardholder(String id, JSONObject details) {
		JSONObject person = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching card key database for IAM ID #" + id + " ...");
		}
		String url = lenelServer + FETCH_URL + id + "%22";
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		get.setHeader("Session-Token", (String) details.get("sessionToken"));
		get.setHeader("Application-Id", lenelApplicationId);
		JSONObject fetchRequest = new JSONObject();
		JSONObject fetchResponse = new JSONObject();
		details.put("fetchRequest", fetchRequest);
		details.put("fetchResponse", fetchResponse);
		fetchRequest.put("url", url);
		try {
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching cardholder data using url " + url);
			}
			HttpResponse resp = client.execute(get);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			fetchResponse.put("responseCode", rc);
			fetchResponse.put("responseString", jsonRespString);
			fetchResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200) {
				String empId = (String) ((JSONObject) details.get("person")).get("SSNO");
				JSONArray records = (JSONArray) result.get("item_list");
				if (records != null && records.size() > 0) {
					person = (JSONObject) ((JSONObject) records.get(0)).get("property_value_map");
					if (records.size() > 1) {
						for (int i=0; i<records.size(); i++) {
							JSONObject thisPerson = (JSONObject) ((JSONObject) records.get(i)).get("property_value_map");
							if (thisPerson.get("SSNO") != null && thisPerson.get("SSNO").equals(empId)) {
								person = thisPerson;
							}
						}
					}
					if (log.isDebugEnabled()) {
						log.debug("Cardholder found for IAM ID " + id + ": " + person.toJSONString());
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Cardholder not found on lenel for IAM ID " + id);
					}
					person = fetchCardholderByEmpId(empId, details);
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
	 * <p>Returns the card holder data on file in the card holder database, if present.</p>
	 *
	 * @param empId the Employee ID of the person
	 * @param details the JSON object containing the details of this transaction
	 * @return the cardholder's data from the card key system
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchCardholderByEmpId(String empId, JSONObject details) {
		JSONObject person = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching card key database for Employee ID #" + empId + " ...");
		}
		String url = lenelServer + FETCH_URL2 + empId + "%22";
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		get.setHeader("Session-Token", (String) details.get("sessionToken"));
		get.setHeader("Application-Id", lenelApplicationId);
		JSONObject fetchRequest = new JSONObject();
		JSONObject fetchResponse = new JSONObject();
		details.put("fetchRequest", fetchRequest);
		details.put("fetchResponse", fetchResponse);
		fetchRequest.put("url", url);
		try {
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching cardholder data using url " + url);
			}
			HttpResponse resp = client.execute(get);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			fetchResponse.put("responseCode", rc);
			fetchResponse.put("responseString", jsonRespString);
			fetchResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200) {
				JSONArray records = (JSONArray) result.get("item_list");
				if (records != null && records.size() > 0) {
					person = (JSONObject) ((JSONObject) records.get(0)).get("property_value_map");
					if (log.isDebugEnabled()) {
						log.debug("Cardholder found for Employee ID #" + empId + ": " + person.toJSONString());
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Cardholder not found on lenel for Employee ID #" + empId);
					}
				}
			} else {
				log.error("Invalid HTTP Response Code returned when fetching Cardholder data for Employee ID #" + empId + ": " + rc);
				eventService.logEvent(new Event(empId, "Cardholder fetch error", "Invalid HTTP Response Code returned when fetching Cardholder data for Employee ID #" + empId + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered when fetching Cardholder data for Employee ID #" + empId + ": " + e, e);
			eventService.logEvent(new Event(empId, "Cardholder fetch exception", "Exception encountered when fetching Cardholder data for Employee ID #" + empId + ": " + e, details, e));
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
	private boolean personUnchanged(HttpServletRequest req, JSONObject person, JSONObject oldPerson) {
		boolean unchanged = false;

		if (isEqual(oldPerson, person, "ADDR1") &&
	 			isEqual(oldPerson, person, "CITY") &&
	 			isEqual(oldPerson, person, "DEPT") &&
	 			isEqual(oldPerson, person, "EMAIL") &&
	 			isEqual(oldPerson, person, "EMP") &&
	 			isEqual(oldPerson, person, "FIRSTNAME") &&
	 			isEqual(oldPerson, person, "FLOOR") &&
	 			isEqual(oldPerson, person, "HR_TITLE") &&
	 			isEqual(oldPerson, person, "HR_DEPT") &&
	 			isEqual(oldPerson, person, "HR_DEPT_ID") &&
	 			isEqual(oldPerson, person, "IAM_ID") &&
	 			isEqual(oldPerson, person, "ID") &&
	 			isEqual(oldPerson, person, "LASTNAME") &&
	 			isEqual(oldPerson, person, "MIDNAME") &&
	 			isEqual(oldPerson, person, "OPHONE") &&
	 			isEqual(oldPerson, person, "PHONE") &&
	 			isEqual(oldPerson, person, "STATE") &&
	 			isEqual(oldPerson, person, "ZIP")) {
			unchanged = true;
		}

		return unchanged;
	}

	/**
	 * <p>Inserts a new Cardholder record.</p>
	 *
	 * @param newPerson the new data for this cardholder
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String insertCardholder(JSONObject newPerson, JSONObject details) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Inserting person " + newPerson.get("IAM_ID"));
		}

		// fix Title
		String title = (String) newPerson.get("TITLE");
		if (StringUtils.isEmpty(title)) {
			newPerson.remove("TITLE");
		} else {
			newPerson.put("TITLE", getTitleId(title, details));
		}

		// create HttpPost
		String url = lenelServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
		post.setHeader("Session-Token", (String) details.get("sessionToken"));
		post.setHeader("Application-Id", lenelApplicationId);

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		insertData.put("type_name", "Lnl_Cardholder");
		insertData.put("property_value_map", newPerson);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + insertData.toJSONString());
		}
		JSONObject insertRequest = new JSONObject();
		JSONObject insertResponse = new JSONObject();
		details.put("insertRequest", insertRequest);
		details.put("insertResponse", insertResponse);
		insertRequest.put("url", url);
		insertRequest.put("insertData", insertData);

		// post parameters
		try {
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting JSON data to " + url);
			}
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			insertResponse.put("responseCode", rc);
			insertResponse.put("responseString", jsonRespString);
			insertResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200 || rc == 201) {
				response = "0;Cardholder inserted";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
				}
			} else {
				response = "2;Unable to insert cardholder";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new cardholder: " + rc);
				}
				eventService.logEvent(new Event((String) details.get("id"), "Cardholder insert error", "Invalid HTTP Response Code returned when inserting new cardholder: " + rc, details));
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new cardholder " + newPerson.get("IAM_ID") + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Cardholder insert exception", "Exception occured when attempting to insert new cardholder " + newPerson.get("IAM_ID") + ": " + e, details, e));
			response = "2;Unable to insert cardholder";
		}

		return response;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param newPerson the new data for this cardholder
	 * @param oldPerson the existing data for this cardholder
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the response
	 */
	@SuppressWarnings("unchecked")
	private String updateCardholder(JSONObject newPerson, JSONObject oldPerson, JSONObject details) {
		String response = null;

		if (log.isDebugEnabled()) {
			log.debug("Updating person " + newPerson.get("IAM_ID"));
		}

		// remove fields that should not be updated
		newPerson.remove("BARCODE1");
		newPerson.remove("FULL_NAME");
		newPerson.remove("TITLE1");
		newPerson.remove("TITLE2");
		newPerson.remove("TITLE3");

		// fix Title
		String title = (String) newPerson.get("TITLE");
		if (StringUtils.isEmpty(title)) {
			newPerson.remove("TITLE");
		} else {
			newPerson.put("TITLE", getTitleId(title, details));
		}

		// create HttpPut
		String url = lenelServer + UPDATE_URL;
		HttpPut put = new HttpPut(url);
		put.setHeader(HttpHeaders.ACCEPT, "application/json");
		put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
		put.setHeader("Session-Token", (String) details.get("sessionToken"));
		put.setHeader("Application-Id", lenelApplicationId);

		// build JSON to put
		JSONObject updateData = new JSONObject();
		JSONObject propertyMap = new JSONObject();
		propertyMap.put("ID", oldPerson.get("ID"));
		Iterator<String> i = newPerson.keySet().iterator();
		while (i.hasNext()) {
			String key = i.next();
			String value = newPerson.get(key) + "";
			if (StringUtils.isNotEmpty(value) && !value.equalsIgnoreCase("null") && !value.equals(oldPerson.get(key))) {
				propertyMap.put(key, value);
			}
		}
		updateData.put("type_name", "Lnl_Cardholder");
		updateData.put("property_value_map", propertyMap);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + updateData.toJSONString());
		}
		JSONObject updateRequest = new JSONObject();
		JSONObject updateResponse = new JSONObject();
		details.put("updateRequest", updateRequest);
		details.put("updateResponse", updateResponse);
		updateRequest.put("url", url);
		updateRequest.put("updateData", updateData);

		// put JSON
		try {
			put.setEntity(new StringEntity(updateData.toJSONString()));
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Putting JSON update to " + url);
			}
			HttpResponse resp = client.execute(put);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			updateResponse.put("responseCode", rc);
			updateResponse.put("responseString", jsonRespString);
			updateResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200) {
				response = "0;Cardholder updated";
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from put: " + rc);
				}
			} else {
				response = "2;Unable to update cardholder";
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when updating IAM ID " + newPerson.get("IAM_ID") + ": " + rc);
				}
				eventService.logEvent(new Event((String) details.get("id"), "User update error", "Invalid HTTP Response Code returned when updating IAM ID " + newPerson.get("IAM_ID") + ": " + rc, details));
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to update IAM ID " + newPerson.get("IAM_ID") + ": " + e);
			eventService.logEvent(new Event((String) details.get("id"), "User update exception", "Exception occured when attempting to update IAM ID " + newPerson.get("IAM_ID") + ": " + e, details, e));
			response = "2;Unable to update cardholder";
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

		person.put("ADDR1", req.getParameter("address"));
		person.put("BARCODE1", req.getParameter("ucPathId"));
//		person.put("BUILDING", req.getParameter("building"));
		person.put("CITY", req.getParameter("city"));
		person.put("DEPT", getDeptId(req.getParameter("deptId"), details));
		person.put("EMAIL", req.getParameter("email"));
		person.put("EMP", req.getParameter("ucPathId"));
		person.put("FIRSTNAME", req.getParameter("firstName"));
		person.put("END_DATE", fixDate(req.getParameter("endDate")));
		person.put("STUDENT_NUMBER", req.getParameter("studentId"));
		person.put("HEALTH_ID", req.getParameter("externalId"));
		person.put("FLOOR", req.getParameter("floor"));
		person.put("HR_TITLE", req.getParameter("title"));
		person.put("HR_DEPT", req.getParameter("deptName"));
		person.put("HR_DEPT_ID", req.getParameter("deptId"));
		person.put("IAM_ID", req.getParameter("id"));
		person.put("LASTNAME", req.getParameter("lastName"));
		person.put("MIDNAME", req.getParameter("middleName"));
		person.put("FULL_NAME", req.getParameter("firstName") + " " + req.getParameter("lastName"));
		person.put("OPHONE", req.getParameter("phoneNumber"));
		person.put("PHONE", req.getParameter("cellNumber"));
		person.put("SSNO", req.getParameter("ucPathId"));
		person.put("START_DATE", fixDate(req.getParameter("startDate")));
		person.put("STATE", req.getParameter("state"));
		person.put("TITLE", req.getParameter("title"));
		person.put("ZIP", req.getParameter("zip"));
		if (StringUtils.isEmpty(req.getParameter("ucPathId")) || !"UCDH".equalsIgnoreCase(req.getParameter("ucPathInstitution"))) {
			person = processExternal(req, person, details);
//		} else {
//			person.put("SPONSOR", null);			
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
				person.put("SSNO", req.getParameter("externalId"));
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
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
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

	/**
	 * <p>Returns the Lenel OnGuard Dept ID for the Tile text passed.</p>
	 *
	 * @param dept the cardholder's dept
	 * @param details the JSON object containing the details of this transaction
	 * @return the Lenel OnGuard Dept ID for the Tile text passed
	 */
	@SuppressWarnings("unchecked")
	private String getDeptId(String dept, JSONObject details) {
		String deptId = null;

		if (StringUtils.isNotEmpty(dept)) {
			if (log.isDebugEnabled()) {
				log.debug("Searching Lenel OnGuard Dept database for Dept name \"" + dept + "\"");
			}
			String url = lenelServer + DEPT_FETCH_URL + dept.replaceAll(" ", "%20") + "%25%22";
			HttpGet get = new HttpGet(url);
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			get.setHeader("Session-Token", (String) details.get("sessionToken"));
			get.setHeader("Application-Id", lenelApplicationId);
			JSONObject deptRequest = new JSONObject();
			JSONObject deptResponse = new JSONObject();
			details.put("deptRequest", deptRequest);
			details.put("deptResponse", deptResponse);
			deptRequest.put("url", url);
			try {
				HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//				HttpClient client = HttpClientProvider.getClient();
				if (log.isDebugEnabled()) {
					log.debug("Fetching Lenel OnGuard Dept data using url " + url);
				}
				HttpResponse resp = client.execute(get);
				int rc = resp.getStatusLine().getStatusCode();
				String jsonRespString = "";
				JSONObject result = new JSONObject();
				HttpEntity entity = resp.getEntity();
				if (entity != null) {
					jsonRespString = EntityUtils.toString(entity);
					result = (JSONObject) JSONValue.parse(jsonRespString);
				}
				deptResponse.put("responseCode", rc);
				deptResponse.put("responseString", jsonRespString);
				deptResponse.put("responseObject", result);
				if (log.isDebugEnabled()) {
					log.debug("JSON response: " + result.toJSONString());
				}
				if (rc == 200) {
					JSONArray records = (JSONArray) result.get("item_list");
					if (records != null && records.size() > 0) {
						JSONObject deptData = (JSONObject) ((JSONObject) records.get(0)).get("property_value_map");
						deptId = deptData.get("ID") + "";
						if (log.isDebugEnabled()) {
							log.debug("Dept ID found for Dept name \"" + dept + "\": " + deptId);
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Dept ID not found for Dept name \"" + dept + "\".");
						}
					}
				} else {
					log.error("Invalid HTTP Response Code returned when searching Lenel OnGuard Dept database for Dept name \"" + dept + "\": " + rc);
					eventService.logEvent(new Event((String) details.get("id"), "Cardholder Dept fetch error", "Invalid HTTP Response Code returned when searching Lenel OnGuard Dept database for Dept name \"" + dept + "\": " + rc, details));
				}
			} catch (Exception e) {
				log.error("Exception encountered when searching Lenel OnGuard Dept database for Dept name \"" + dept + "\": " + e, e);
				eventService.logEvent(new Event((String) details.get("id"), "Cardholder Dept fetch exception", "Exception encountered when searching Lenel OnGuard Dept database for Dept name \"" + dept + "\": " + e, details, e));
			}
			if (log.isDebugEnabled()) {
				log.debug("Returning Dept ID: " + deptId);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Incoming Department ID is blank; Department search bypassed.");
			}
		}

		return deptId;
	}

	/**
	 * <p>Returns the Lenel OnGuard Title ID for the Tile text passed.</p>
	 *
	 * @param title the cardholder's title
	 * @param details the JSON object containing the details of this transaction
	 * @return the Lenel OnGuard Title ID for the Tile text passed
	 */
	@SuppressWarnings("unchecked")
	private String getTitleId(String title, JSONObject details) {
		String titleId = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching Lenel OnGuard Title database for Title text \"" + title + "\"");
		}
		String url = lenelServer + TITLE_FETCH_URL + title.replaceAll(" ", "%20") + "%22";
		HttpGet get = new HttpGet(url);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		get.setHeader("Session-Token", (String) details.get("sessionToken"));
		get.setHeader("Application-Id", lenelApplicationId);
		JSONObject titleRequest = new JSONObject();
		JSONObject titleResponse = new JSONObject();
		details.put("titleRequest", titleRequest);
		details.put("titleResponse", titleResponse);
		titleRequest.put("url", url);
		try {
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching Lenel OnGuard Title data using url " + url);
			}
			HttpResponse resp = client.execute(get);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			titleResponse.put("responseCode", rc);
			titleResponse.put("responseString", jsonRespString);
			titleResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200) {
				JSONArray records = (JSONArray) result.get("item_list");
				if (records != null && records.size() > 0) {
					JSONObject titleData = (JSONObject) ((JSONObject) records.get(0)).get("property_value_map");
					titleId = titleData.get("ID") + "";
					if (log.isDebugEnabled()) {
						log.debug("Title ID found for Title text \"" + title + "\": " + titleId);
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Title ID not found for Title text \"" + title + "\"; creating new Title record.");
					}
					titleId = createNewTitleId(title, details);
				}
			} else {
				log.error("Invalid HTTP Response Code returned when searching Lenel OnGuard Title database for Title text \"" + title + "\": " + rc);
				eventService.logEvent(new Event((String) details.get("id"), "Cardholder Title fetch error", "Invalid HTTP Response Code returned when searching Lenel OnGuard Title database for Title text \"" + title + "\": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered when searching Lenel OnGuard Title database for Title text \"" + title + "\": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Cardholder Title fetch exception", "Exception encountered when searching Lenel OnGuard Title database for Title text \"" + title + "\": " + e, details, e));
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning Title ID: " + titleId);
		}

		return titleId;
	}

	/**
	 * <p>Creates a new Lenel OnGuard Title record for the Tile text passed and returns the Title ID.</p>
	 *
	 * @param title the cardholder's title
	 * @param details the JSON object containing the details of this transaction
	 * @return the Lenel OnGuard Title ID for the Tile text passed
	 */
	@SuppressWarnings("unchecked")
	private String createNewTitleId(String title, JSONObject details) {
		String titleId = null;

		if (log.isDebugEnabled()) {
			log.debug("Creating a new Lenel OnGuard Title record for Title text \"" + title + "\"");
		}

		// create HttpPost
		String url = lenelServer + UPDATE_URL;
		HttpPost post = new HttpPost(url);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		post.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
		post.setHeader("Session-Token", (String) details.get("sessionToken"));
		post.setHeader("Application-Id", lenelApplicationId);

		// build JSON object to post
		JSONObject insertData = new JSONObject();
		JSONObject titleData = new JSONObject();
		titleData.put("Name", title);
		insertData.put("type_name", "Lnl_TITLE");
		insertData.put("property_value_map", titleData);
		if (log.isDebugEnabled()) {
			log.debug("JSON object to POST: " + insertData.toJSONString());
		}
		JSONObject titleInsertRequest = new JSONObject();
		JSONObject titleInsertResponse = new JSONObject();
		details.put("titleInsertRequest", titleInsertRequest);
		details.put("titleInsertResponse", titleInsertResponse);
		titleInsertRequest.put("url", url);
		titleInsertRequest.put("titleInsertData", insertData);

		// post parameters
		try {
			post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), post, null));
			post.setEntity(new StringEntity(insertData.toJSONString()));
			HttpClient client = createHttpClient_AcceptsUntrustedCerts();
//			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Posting JSON data to " + url);
			}
			HttpResponse resp = client.execute(post);
			int rc = resp.getStatusLine().getStatusCode();
			String jsonRespString = "";
			JSONObject result = new JSONObject();
			HttpEntity entity = resp.getEntity();
			if (entity != null) {
				jsonRespString = EntityUtils.toString(entity);
				result = (JSONObject) JSONValue.parse(jsonRespString);
			}
			titleInsertResponse.put("responseCode", rc);
			titleInsertResponse.put("responseString", jsonRespString);
			titleInsertResponse.put("responseObject", result);
			if (log.isDebugEnabled()) {
				log.debug("JSON response: " + result.toJSONString());
			}
			if (rc == 200 || rc == 201) {
				if (log.isDebugEnabled()) {
					log.debug("HTTP response code from post: " + rc);
					log.debug("JSON response: " + jsonRespString);
				} 
				titleData = (JSONObject) result.get("property_value_map");
				if (titleData != null) {
					titleId = titleData.get("ID") + "";
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when inserting new Title record: " + rc);
					log.debug("JSON response: " + jsonRespString);
				}
				eventService.logEvent(new Event((String) details.get("id"), "Title insert error", "Invalid HTTP Response Code returned when inserting new Title: " + rc, details));
			}
		} catch (Exception e) {
			log.debug("Exception occured when attempting to insert new Title: " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "Title insert exception", "Exception occured when attempting to insert new Title: " + e, details, e));
		}

		return titleId;
	}

	public String fixDate(String dateString) {
		String date = null;

		if (StringUtils.isNotEmpty(dateString) && dateString.length() > 9) {
			dateString = dateString.substring(0, 10);
			String [] parts = dateString.split("-");
			if (parts.length == 3) {
				LocalDateTime ldt = LocalDateTime.of(Integer.valueOf(parts[0]).intValue(), Integer.valueOf(parts[1]).intValue(), Integer.valueOf(parts[2]).intValue(), 0, 0);
				ZonedDateTime zdt = ZonedDateTime.of(ldt, ZoneId.of("America/Los_Angeles"));
				date = dateString + "T00:00:00" + zdt.getOffset();
			}
		}

		return date;
	}

	@SuppressWarnings("deprecation")
	public HttpClient createHttpClient_AcceptsUntrustedCerts() throws KeyManagementException, NoSuchAlgorithmException, KeyStoreException {
	    HttpClientBuilder b = HttpClientBuilder.create();
	 
	    // setup a Trust Strategy that allows all certificates.
	    //
	    SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
	        public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
	            return true;
	        }
	    }).build();
	    b.setSslcontext( sslContext);
	 
	    // don't check Hostnames, either.
	    //      -- use SSLConnectionSocketFactory.getDefaultHostnameVerifier(), if you don't want to weaken
	    HostnameVerifier hostnameVerifier = SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER;
	 
	    // here's the special part:
	    //      -- need to create an SSL Socket Factory, to use our weakened "trust strategy";
	    //      -- and create a Registry, to register it.
	    //
	    SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);
	    Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
	            .register("http", PlainConnectionSocketFactory.getSocketFactory())
	            .register("https", sslSocketFactory)
	            .build();
	 
	    // now, we create connection-manager using our Registry.
	    //      -- allows multi-threaded use
	    PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager( socketFactoryRegistry);
	    b.setConnectionManager( connMgr);
	 
	    // finally, build the HttpClient;
	    //      -- done!
	    HttpClient client = b.build();
	    return client;
	}

	private boolean isEqual(JSONObject p1, JSONObject p2, String key) {
		boolean eq = true;

		String s1 = p1.get(key) + "";
		String s2 = p2.get(key) + "";
		if (StringUtils.isNotEmpty(s1) && !"null".equalsIgnoreCase(s1)) {
			if (StringUtils.isNotEmpty(s2) && !"null".equalsIgnoreCase(s2)) {
				eq = s1.equals(s2);
			}
		} else {
			if (StringUtils.isNotEmpty(s2) && !"null".equalsIgnoreCase(s2)) {
				eq = false;
			}
		}

		return eq;
	}
/*
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
 */
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
