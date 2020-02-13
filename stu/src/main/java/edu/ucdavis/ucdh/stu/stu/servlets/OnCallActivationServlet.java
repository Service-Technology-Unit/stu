package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.stu.beans.Contact;

/**
 * <p>This servlet handles the on-call activation service.</p>
 */
public class OnCallActivationServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private static final String RESPONSE_CODE = "responseCode";
	private static final String RESPONSE = "response";
	private static final String FETCH_URL = "/api/now/table/u_temporary_on_call?sysparm_display_value=all&sysparm_query=u_group%3D";
	private static final String UPDATE_URL = "/api/now/table/u_temporary_on_call";
	private static final DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/oncall.js";
		ServletConfig config = getServletConfig();
		serviceNowServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowServer");
		serviceNowUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowUser");
		serviceNowPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowPassword");
	}

	/**
	 * <p>The processRequest method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	@SuppressWarnings("unchecked")
	public String processRequest(HttpServletRequest req, HttpServletResponse res) throws IOException {
		JSONObject response = new JSONObject();

		JSONObject details = new JSONObject();
		details.put("groups", new JSONObject());
		Contact user = (Contact) req.getSession().getAttribute(USER_DETAILS);

		if (user != null) {
			details.put("userSysId", user.getSysId());
			details.put("userId", user.getId());
			details.put("user", user.getFirstName() + " " + user.getLastName());
			String contactMethod = req.getParameter("method");
			String alternateMethod = req.getParameter("altmethod");
			String groups = req.getParameter("groups");
			if (StringUtils.isEmpty(contactMethod)) {
				response = setResponse("2", "Unable to determine contact method for user " + user.getFirstName() + " " + user.getLastName());
			} else {
				if (groups != null) {
					String[] group = groups.split(",");
					List<String> successGroup = new ArrayList<String>();
					List<String> failureGroup = new ArrayList<String>();
					for (int i=0; i<group.length; i++) {
						String thisGroup = group[i];
						String result = activateGroup(user, thisGroup, contactMethod, alternateMethod, details);
						if (result.equals(thisGroup)) {
							successGroup.add((String) ((JSONObject) details.get("groups")).get(thisGroup));
						} else {
							failureGroup.add((String) ((JSONObject) details.get("groups")).get(thisGroup));
						}
					}
					if (successGroup.size() > 0) {
						String successMessage = "User " + user.getFirstName() + " " + user.getLastName() + " has been activated as the on-call for the following Assignment Groups: ";
						String separator = "";
						for (int i=0; i<successGroup.size(); i++) {
							successMessage += separator;
							successMessage += successGroup.get(i);
							separator = ", ";
						}
						if (failureGroup.size() > 0) {
							String failureMessage = "Unable to activate user " + user.getFirstName() + " " + user.getLastName() + " for the following Assignment Groups: ";
							separator = "";
							for (int i=0; i<failureGroup.size(); i++) {
								failureMessage += separator;
								failureMessage += failureGroup.get(i);
								separator = ", ";
							}
							response = setResponse("1", successMessage + "; " + failureMessage);
						} else {
							response = setResponse("0", successMessage);
						}
					} else {
						String failureMessage = "Unable to activate user " + user.getFirstName() + " " + user.getLastName() + " for the following Assignment Groups: ";
						String separator = "";
						for (int i=0; i<failureGroup.size(); i++) {
							failureMessage += separator;
							failureMessage += failureGroup.get(i);
							separator = ", ";
						}
						response = setResponse("2", failureMessage);
					}
				} else {
					response = setResponse("2", "No Assignment Groups identified for activation");
				}
			}
		} else {
			response = setResponse("2", "No currently authenticated user.");
		}

		return response.toJSONString();
	}

	/**
	 * <p>Activates the authenticated user as the on-call for the specified group.</p>
	 *
	 * @param user the authenticated user
	 * @param group the Assignment Group
	 * @param contactMethod the preferred contact method
	 * @param alternateMethod the alternate contact method
	 * @return the outcome of  the activation attempt
	 */
	@SuppressWarnings("unchecked")
	private String activateGroup(Contact user, String group, String contactMethod, String alternateMethod, JSONObject details) {
		String outcome = group;

		JSONObject groupInfo = fetchGroupInfo(group, details);
		if (groupInfo != null) {
			String sysId = getValue(groupInfo, "sys_id", false);
			String groupName = getValue(groupInfo, "u_group", true);
			String userSysId = getValue(groupInfo, "u_user", false);
			String currentMethod = getValue(groupInfo, "u_method", true);
			String currentAltMethod = getValue(groupInfo, "u_alternate_method", true);
			if (userSysId.equalsIgnoreCase((String) details.get("userSysId")) && currentMethod.equalsIgnoreCase(currentMethod) && currentAltMethod.equalsIgnoreCase(alternateMethod)) {
				outcome += " (Specified user is already the designated on-call with this contact method)";
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Activating " + details.get("user") + " as the on-call for " + groupName);
				}

				// create HttpPut
				String url = serviceNowServer + UPDATE_URL + "/" + sysId;
				HttpPut put = new HttpPut(url);
				put.setHeader(HttpHeaders.ACCEPT, "application/json");
				put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

				// build JSON to put
				JSONObject updateData = new JSONObject();
				updateData.put("u_user", details.get("userSysId"));
				updateData.put("u_method", contactMethod);
				updateData.put("u_alternate_method", alternateMethod);
				updateData.put("u_since", DF.format(new Date()));
				if (log.isDebugEnabled()) {
					log.debug("JSON object to PUT: " + updateData.toJSONString());
				}

				// put JSON
				try {
					put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
					put.setEntity(new StringEntity(updateData.toJSONString()));
					HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
					if (log.isDebugEnabled()) {
						log.debug("Putting JSON update to " + url);
					}
					HttpResponse resp = client.execute(put);
					int rc = resp.getStatusLine().getStatusCode();
					if (rc == 200) {
						if (log.isDebugEnabled()) {
							log.debug("HTTP response code from put: " + rc);
						}
					} else {
						outcome = "Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc;
						if (log.isDebugEnabled()) {
							log.debug("Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc);
						}
						eventService.logEvent(new Event((String) details.get("id"), "User update error", "Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc, details));
					}
					if (log.isDebugEnabled()) {
						String jsonRespString = "";
						HttpEntity entity = resp.getEntity();
						if (entity != null) {
							jsonRespString = EntityUtils.toString(entity);
						}
						JSONObject result = (JSONObject) JSONValue.parse(jsonRespString);
						log.debug("JSON response: " + result.toJSONString());
					}
				} catch (Exception e) {
					log.debug("Exception occured when attempting to update group " + groupName + ": " + e);
					eventService.logEvent(new Event((String) details.get("id"), "On-call update exception", "Exception occured when attempting to update group " + groupName + ": " + e, details, e));
					outcome = "Exception occured when attempting to update group " + groupName + ": " + e;
				}
			}
		} else {
			outcome += " (Invalid Assignment Group)";
		}

		return outcome;
	}

	/**
	 * <p>Returns the information on the specified Assignment Group.</p>
	 *
	 * @param group the Assignment Group name
	 * @return the information on the specified Assignment Group
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchGroupInfo(String group, JSONObject details) {
		JSONObject groupInfo = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching group information for " + group);
		}
		String url = serviceNowServer + FETCH_URL + group;
		HttpGet get = new HttpGet(url);
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
			if (log.isDebugEnabled()) {
				log.debug("Fetching group data using url " + url);
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
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "Group fetch error", "Invalid HTTP Response Code returned when fetching group information for group " + group + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching group information for group " + group + ": " + rc);
				}
			}
			JSONArray groups = (JSONArray) result.get("result");
			if (groups != null && groups.size() > 0) {
				groupInfo = (JSONObject) groups.get(0);
				((JSONObject) details.get("groups")).put(group, getValue(groupInfo, "u_group", true));
				if (log.isDebugEnabled()) {
					log.debug("Group info found for group " + group + ": " + groupInfo.toJSONString());
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Group info not found for group " + group);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for group information for group " + group + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for group information for group " + group + ": " + e, details, e));
		}

		return groupInfo;
	}

	private String getValue(JSONObject json, String key, boolean displayValue) {
		String response = "";

		JSONObject info = (JSONObject) json.get(key);
		if (info != null) {
			if (displayValue) {
				response = (String) info.get("display_value");
			} else {
				response = (String) info.get("value");
			}
		}

		return response;
	}

	/**
	 * <p>Formats a response object.</p>
	 *
	 * @param responseCode the responseCode
	 * @param response the response
	 * @return the formatted response
	 */
	@SuppressWarnings("unchecked")
	private JSONObject setResponse(String responseCode, String response) {
		JSONObject json = new JSONObject();

		json.put(RESPONSE_CODE, responseCode);
		json.put(RESPONSE, response);

		return json;
	}
}