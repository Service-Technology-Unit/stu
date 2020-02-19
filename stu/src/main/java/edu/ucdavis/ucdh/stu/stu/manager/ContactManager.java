package edu.ucdavis.ucdh.stu.stu.manager;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

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

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.snutil.util.EventService;
import edu.ucdavis.ucdh.stu.stu.beans.Contact;

/**
 * <p>This is the Contact manager.</p>
 */
public class ContactManager {
	private Log log = LogFactory.getLog(getClass());
	private static final String FETCH_URL = "/api/now/table/sys_user?sysparm_display_value=all&sysparm_query=u_kerberos_id%3D";
	private static final String GROUP_FETCH_URL = "/api/now/table/sys_user_grmember?sysparm_display_value=true&sysparm_fields=group.name&sysparm_query=user%3D";
	private EventService eventService = null;
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;

	/**
	 * <p>Returns the Contact with the specified id.</p>
	 *
	 * @param id the id of the requested contact
	 * @return the Contact with the specified id
	 */
	@SuppressWarnings("unchecked")
	public Contact getContact(String id) {
		Contact contact = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching contact information for Kerberos ID " + id);
		}
		JSONObject details = new JSONObject();
		details.put("id", id);
		JSONObject user = fetchServiceNowUser(id, details);
		if (user != null) {
			details.put("user", user);
			contact = populateContact(user, details);
		}

		return contact;
	}

	private Contact populateContact(JSONObject user, JSONObject details) {
		Contact contact = new Contact();

		if (log.isDebugEnabled()) {
			log.debug("Populating contact information for Kerberos ID " + details.get("id"));
		}
		contact.setId(getValue(user, "u_kerberos_id", true));
		contact.setSysId(getValue(user, "sys_id", true));
		contact.setEmployeeId(getValue(user, "employee_number", true));
		contact.setUcdLoginId(getValue(user, "user_name", true));
		contact.setLastName(getValue(user, "last_name", true));
		contact.setFirstName(getValue(user, "first_name", true));
		contact.setMiddleName(getValue(user, "middle_name", true));
		contact.setTitle(getValue(user, "title", true));
		contact.setDepartment(getValue(user, "department", true));
		contact.setSupervisor(getValue(user, "u_supervisor", true));
		contact.setManager(getValue(user, "manager", true));
		contact.setEmail(getValue(user, "email", true));
		contact.setAlternateEmail(getValue(user, "u_alternate_email", true));
		contact.setPhoneNr(getValue(user, "phone", true));
		contact.setCellPhone(getValue(user, "mobile_phone", true));
		contact.setPager(getValue(user, "u_pager", true));
		contact.setPagerProvider(getValue(user, "u_pager_provider", true));
		contact.setAlternatePhones(getValue(user, "u_alternate_phones", true));
		contact.setLocationName(getValue(user, "location", true));
		contact.setStation(getValue(user, "u_cube", true));
		contact.setActive(getValue(user, "active", true));
		contact.setValidFrom(getDate(getValue(user, "u_valid_from", false)));
		contact.setValidTo(getDate(getValue(user, "u_valid_to", false)));
		contact.setCreationDate(getDate(getValue(user, "sys_created_on", false)));
		contact.setCreatedBy(getValue(user, "sys_created_by", true));
		contact.setLastUpdate(getDate(getValue(user, "sys_updated_on", false)));
		contact.setLastUpdateBy(getValue(user, "sys_updated_by", true));
		contact.setGroup(getUserGroups(getValue(user, "sys_id", false), details));
		if (contact.getGroup().contains("IT Staff")) {
			contact.setItStaff("t");
		}

		if (log.isDebugEnabled()) {
			log.debug("Completed population of contact information for Kerberos ID " + details.get("id"));
		}

		return contact;
	}

	/**
	 * <p>Returns the ServiceNow user data on file, if present.</p>
	 *
	 * @param id the Kerberos ID of the person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow user data
	 */
	@SuppressWarnings("unchecked")
	private JSONObject fetchServiceNowUser(String id, JSONObject details) {
		JSONObject user = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow user data for Kerberos ID " + id);
		}
		String url = serviceNowServer + FETCH_URL + id;
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
			JSONObject result = (JSONObject) JSONValue.parse(resp);
			if (rc != 200) {
				details.put("responseCode", rc + "");
				details.put("responseBody", result);
				eventService.logEvent(new Event((String) details.get("id"), "User fetch error", "Invalid HTTP Response Code returned when fetching user data for Kerberos ID " + id + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user data for Kerberos ID " + id + ": " + rc);
				}
			}
			JSONArray users = (JSONArray) result.get("result");
			if (users != null && users.size() > 0) {
				user = (JSONObject) users.get(0);
				if (log.isDebugEnabled()) {
					log.debug("User found for Kerberos ID " + id + ": " + user);
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User not found for Kerberos ID " + id);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user with Kerberos ID " + id + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User fetch exception", "Exception encountered searching for user with Kerberos ID " + id + ": " + e, details, e));
		}

		return user;
	}

	/**
	 * <p>Returns the ServiceNow user data on file, if present.</p>
	 *
	 * @param sysId the ServiceNow sys_id of the person
	 * @param details the <code>JSONObject</code> object containing the details of the request
	 * @return the ServiceNow user data
	 */
	@SuppressWarnings("unchecked")
	private List<String> getUserGroups(String sysId, JSONObject details) {
		List<String> groups = new ArrayList<String>();

		if (log.isDebugEnabled()) {
			log.debug("Fetching ServiceNow user group data for sys_id " + sysId);
		}
		String url = serviceNowServer + GROUP_FETCH_URL + sysId;
		HttpGet get = new HttpGet(url);
		try {
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			if (log.isDebugEnabled()) {
				log.debug("Fetching user group data using url " + url);
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
				eventService.logEvent(new Event((String) details.get("id"), "User group fetch error", "Invalid HTTP Response Code returned when searching for groups for user with sys_id " + sysId + ": " + rc, details));
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching user group data for sys_id " + sysId + ": " + rc);
				}
			}
			JSONArray group = (JSONArray) result.get("result");
			if (group != null && group.size() > 0) {
				details.put("group", group);
				if (log.isDebugEnabled()) {
					log.debug("User groups found for sys_id " + sysId + ": " + group);
				}
				Iterator<Object> i = group.iterator();
				while (i.hasNext()) {
					groups.add((String) ((JSONObject) i.next()).get("group.name"));
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User groups not found for sys_id " + sysId);
				}
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for user with sys_id " + sysId + ": " + e, e);
			eventService.logEvent(new Event((String) details.get("id"), "User group fetch exception", "Exception encountered searching for groups for user with sys_id " + sysId + ": " + e, details, e));
		}

		return groups;
	}

	private static String getValue(JSONObject object, String property, boolean displayValue) {
		String value = null;

		JSONObject info = (JSONObject) object.get(property);
		if (info != null) {
			String key = "value";
			if (displayValue) {
				key = "display_value";
			}
			value = (String) info.get(key);
		}

		return value;
	}

	private static Date getDate(String dateString) {
		Date date = null;

		if (StringUtils.isNotEmpty(dateString)) {
//			date = new Date(dateString);
		}

		return date;
	}

	public EventService getEventService() {
		return eventService;
	}

	public void setEventService(EventService eventService) {
		this.eventService = eventService;
	}

	public String getServiceNowServer() {
		return serviceNowServer;
	}

	public void setServiceNowServer(String serviceNowServer) {
		this.serviceNowServer = serviceNowServer;
	}

	public String getServiceNowUser() {
		return serviceNowUser;
	}

	public void setServiceNowUser(String serviceNowUser) {
		this.serviceNowUser = serviceNowUser;
	}

	public String getServiceNowPassword() {
		return serviceNowPassword;
	}

	public void setServiceNowPassword(String serviceNowPassword) {
		this.serviceNowPassword = serviceNowPassword;
	}
}