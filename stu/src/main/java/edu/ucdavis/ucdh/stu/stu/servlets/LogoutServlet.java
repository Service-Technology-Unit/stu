package edu.ucdavis.ucdh.stu.stu.servlets;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;

/**
 * <p>This servlet validates a CAS authentication ticket and converts the response to Javascript.</p>
 */
public class LogoutServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private static final String AUTHENTICATED_USER = "authenticatedUser";

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/logout.js";
		defaultVar = "logoutSuccessful";
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 */
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		String string = "false";

		if (StringUtils.isNotEmpty((String) req.getSession().getAttribute(AUTHENTICATED_USER))) {
			req.getSession().removeAttribute(AUTHENTICATED_USER);
			string = "true";
		}

		return string;
	}
}
