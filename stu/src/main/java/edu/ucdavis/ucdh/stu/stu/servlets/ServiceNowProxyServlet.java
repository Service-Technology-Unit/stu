package edu.ucdavis.ucdh.stu.stu.servlets;

import java.nio.charset.StandardCharsets;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.stu.beans.Contact;

/**
 * <p>This servlet serves as a proxy to the ServiceNow web services.</p>
 */
public class ServiceNowProxyServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private String serviceEndPoint = null;
	private String serviceAccount = null;
	private String serviceCredentials = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/webservice.js";
		ServletConfig config = getServletConfig();
		serviceEndPoint = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("snWebServiceEndPoint");
		serviceAccount = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("snWebServiceAccount");
		serviceCredentials = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("snWebServiceCredentials");
		if (serviceEndPoint.endsWith("/")) {
			serviceEndPoint = serviceEndPoint.substring(0, serviceEndPoint.length() - 1);
		}
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 */
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		String string = "";

		String service = req.getParameter("service");
		String method = req.getParameter("method");
		String content = req.getParameter("content");
		Contact userDetails = (Contact) req.getSession().getAttribute(USER_DETAILS);
		if (userDetails != null) {
			if (StringUtils.isNotEmpty(service)) {
				string = invokeWebService(userDetails, service, method, content);
			} else {
				string = "{\"Messages\": [\"No web service specified.\"],\"ReturnCode\": 999}";
			}
		} else {
			string = "{\"Messages\": [\"Access denied to unauthenticated user.\"],\"ReturnCode\": 998}";
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning the following value from the web service process: " + string);
			log.debug("User Agent: " + req.getHeader("User-Agent"));
		}

		return string;
	}

	/**
	 * <p>Invokes the web service using the parameters provided.</p>
	 *
	 * @param userDetails the details on the currently authenticated user
	 * @param service the web service to invoke
	 * @param method the HTTP method to use
	 * @param content the content to send
	 * @return the response from the web service
	 */
	private String invokeWebService(Contact userDetails, String service, String method, String content) {
		String string = "";

		if (!service.startsWith("/")) {
			service = "/" + service;
		}
		String url = serviceEndPoint + service;
		if ("post".equalsIgnoreCase(method)) {
			if (log.isDebugEnabled()) {
				log.debug("Posting content to URL " + url + " on behalf of " + userDetails.getFirstName() + " " + userDetails.getLastName());
			}
			try {
				HttpClient client = HttpClientProvider.getClient();
				HttpPost post = new HttpPost(url);
				post.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceAccount, serviceCredentials), post, null));
				if (StringUtils.isNotEmpty(content)) {
					post.setEntity(new ByteArrayEntity(content.getBytes("UTF-8")));
				}
				HttpResponse response = client.execute(post);
				if (response.getStatusLine().getStatusCode() == 200) {
					string = EntityUtils.toString(response.getEntity());
				} else {
					int rc = response.getStatusLine().getStatusCode();
					//log.error("Invalid response code (" + rc + ") encountered posting to URL " + url);
					log.error("Invalid response code (" + rc + ") encountered posting to URL " + url + "; response: " + EntityUtils.toString(response.getEntity()));
					string = "{\"Messages\": [\"Invalid HTTP response code returned from web service: " + rc + "\"],\"ReturnCode\": " + rc + "}";
				}
			} catch (Exception e) {
				log.error("Exception encountered posting to URL " + url, e);
				string = "{\"Messages\": [\"Proxy exception: " + e + "\"],\"ReturnCode\": 997}";
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Accessing URL " + url + " on behalf of " + userDetails.getFirstName() + " " + userDetails.getLastName());
			}
			try {
				HttpClient client = HttpClientProvider.getClient();
				HttpGet get = new HttpGet(url);
				get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceAccount, serviceCredentials), get, null));
				HttpResponse response = client.execute(get);
				int rc = response.getStatusLine().getStatusCode();
				if (rc == 200) {
					string = EntityUtils.toString(response.getEntity());
				} else if (rc == 404) {
					string = "{\"Messages\": [\"The requested record was not found.\"],\"ReturnCode\": " + rc + "}";
				} else {
					log.error("Invalid response code (" + rc + ") encountered accessing URL " + url);
					string = "{\"Messages\": [\"Invalid HTTP response code returned from web service: " + rc + "\"],\"ReturnCode\": " + rc + "}";
				}
			} catch (Exception e) {
				log.error("Exception encountered accessing URL " + url, e);
				string = "{\"Messages\": [\"Proxy exception: " + e + "\"],\"ReturnCode\": 997}";
			}
		}

		return string;
	}
}
