package edu.ucdavis.ucdh.stu.stu.servlets;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.stu.beans.Contact;
import edu.ucdavis.ucdh.stu.stu.manager.ContactManager;

/**
 * <p>This servlet validates a CAS authentication ticket and converts the response to Javascript.</p>
 */
public class CasTicketValidationServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private ContactManager contactManager = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/validate.js";
		defaultVar = "userId";
		ServletConfig config = getServletConfig();
		contactManager = (ContactManager) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("contactManager");
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

		req.getSession().removeAttribute(AUTHENTICATED_USER);
		String url = req.getParameter("url");
		if (StringUtils.isNotEmpty(url)) {
			if (url.indexOf(" ") != -1) {
				url = url.replaceAll(" ", "%20");
			}
			if (log.isDebugEnabled()) {
				log.debug("Processing URL " + url);
			}
			String userId = processURL(url);
			if (StringUtils.isNotEmpty(userId)) {
				Contact userDetails = getUserDetails(userId);
				if (userDetails != null) {
					req.getSession().setAttribute(AUTHENTICATED_USER, userId);
					req.getSession().setAttribute(USER_DETAILS, userDetails);
					string = "'" + userId + "'";
				} else {
					string = "null";
				}
			} else {
				string = "''";
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning the following value from the authentication process: " + string);
			log.debug("User Agent: " + req.getHeader("User-Agent"));
		}

		return string;
	}

	/**
	 * <p>Calls the URL and interprets the results.</p>
	 *
	 * @param url the URL to call
	 * @return the id of the authenticated user, if the ticket is valid
	 */
	private String processURL(String url) {
		String string = "";

		HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
		try {
			HttpGet get = new HttpGet(url);
			HttpResponse response = client.execute(get);
			if (response.getStatusLine().getStatusCode() == 200) {
				String xml = EntityUtils.toString(response.getEntity());
				if (log.isDebugEnabled()) {
					log.debug("XML Response from CAS server: " + xml);
				}
				if (xml.indexOf("<cas:user>") != -1) {
					string = xml.substring(xml.indexOf("<cas:user>") + 10);
					string = string.substring(0, string.indexOf("<"));
				} else {
					log.warn("Invalid response (" + xml + ") encountered accessing to URL " + url);
				}
			} else {
				log.error("Invalid response code (" + response.getStatusLine().getStatusCode() + ") encountered accessing to URL " + url);
			}
		} catch (Exception e) {
			log.error("Exception encountered accessing URL " + url, e);
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning the following value from the CAS response processing: " + string);
		}

		return string;
	}

	/**
	 * <p>Validates the User ID.</p>
	 *
	 * @param userId the userId to validate
	 * @return the id of the authenticated user, if the user id is valid
	 */
	private Contact getUserDetails(String userId) {
		Contact thisUser = contactManager.getContact(userId);
		if (thisUser != null) {
			if (log.isDebugEnabled()) {
				log.debug("AD login ID is " + thisUser.getId());
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("User not found for UCD login ID " + userId);
			}
		}
		return thisUser;
	}
}
