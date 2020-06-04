package edu.ucdavis.ucdh.stu.stu.servlets;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

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
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;

/**
 * <p>This servlet produces the Javascript for a list of locations.</p>
 */
public class LocationServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private static final String FETCH_URL = "/api/now/table/cmn_location?sysparm_fields=city%2Ccountry%2Cfull_name%2Clatitude%2Clongitude%2Cname%2Cparent.u_location_code%2Cstate%2Cstreet%2Csys_id%2Cu_location_code%2Czip&sysparm_query=ORDERBYname%5Eparent";
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		servletPath = "/children.js";
		defaultVar = "children";
		serviceNowServer = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowServer");
		serviceNowUser = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowUser");
		serviceNowPassword = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("serviceNowPassword");
	}

	/**
	 * <p>Returns the Javascript object for the list of locations.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript object for the list of locations
	 */
	@SuppressWarnings("unchecked")
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		String response = "[]";

		if (log.isDebugEnabled()) {
			log.debug("Fetching location data ...");
		}
		JSONObject details = new JSONObject();
		details.put("parentId", req.getParameter("parent"));
		JSONArray locations = getLocations(req.getParameter("parent"), details);
		if (locations != null && locations.size() > 0) {
			JSONArray children = new JSONArray();
			Iterator<JSONObject> i = locations.iterator();
			while (i.hasNext()) {
				JSONObject location = i.next();
				JSONObject child = new JSONObject();
				String name = (String) location.get("name");
				String shortName = name;
				if (name.indexOf(", ") != -1) {
					shortName = name.substring(name.indexOf(", ") + 2);
				}
				child.put("id", location.get("u_location_code"));
				child.put("city", location.get("city"));
				child.put("country", location.get("country"));
				child.put("fullName", name);
				child.put("latitude", location.get("latitude"));
				child.put("longitude", location.get("longitude"));
				child.put("name", name);
				child.put("parent", location.get("parent.u_location_code"));
				child.put("state", location.get("state"));
				child.put("address", location.get("street"));
				child.put("sys_id", location.get("sys_id"));
				child.put("zip", location.get("zip"));
				child.put("shortName", shortName);
				children.add(child);
			}
			response = children.toJSONString();
		}

		return response;
	}

	/**
	 * <p>Fetches the location data from ServiceNow.</p>
	 *
	 * @param locationId
	 * @param details
	 * @return the array of ServiceNow Location Identity records
	 */
	@SuppressWarnings("unchecked")
	private JSONArray getLocations(String locationId, JSONObject details) {
		JSONArray records = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching location data from ServiceNow for Location ID " + locationId);
		}
		String url = serviceNowServer + FETCH_URL + "ISEMPTY";
		if (StringUtils.isNotEmpty(locationId)) {
			url = serviceNowServer + FETCH_URL + ".u_location_code%3D" + locationId;
		}
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
						JSONObject location = (JSONObject) records.get(0);
						if (log.isDebugEnabled()) {
							log.debug("Location found for Location ID " + locationId + ": " + location.toJSONString());
						}
					} else {
						if (log.isDebugEnabled()) {
							log.debug("Location not found on ServiceNow for Location ID " + locationId);
						}
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Location not found on ServiceNow for Location ID " + locationId);
					}
				}
			} else {
				details.put("responseCode", rc + "");
				details.put("responseBody", resp);
				log.error("Invalid HTTP Response Code returned when fetching location data for Location ID " + locationId + ": " + rc);
				eventService.logEvent(new Event(locationId, "Identity fetch error", "Invalid HTTP Response Code returned when fetching location data for Location ID " + locationId + ": " + rc, details));
			}
		} catch (Exception e) {
			log.error("Exception encountered searching for location with Location ID " + locationId + ": " + e, e);
			eventService.logEvent(new Event(locationId, "Identity fetch exception", "Exception encountered searching for location with Location ID " + locationId + ": " + e, details, e));
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning Location data: " + records);
		}

		return records;
	}
}
