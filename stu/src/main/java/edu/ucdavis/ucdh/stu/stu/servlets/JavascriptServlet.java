package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.snutil.util.EventService;

/**
 * <p>This is the base class for Javascript servlets.</p>
 */
public abstract class JavascriptServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	protected static final String AUTHENTICATED_USER = "authenticatedUser";
	protected static final String USER_DETAILS = "userDetails";
	protected static final String USER_GROUPS = "userGroups";
	protected static final String USER_TEAMS = "userTeams";
	protected Log log = LogFactory.getLog(getClass());
	protected EventService eventService = null;
	protected boolean securedApplication = false;
	protected String servletPath = null;
	protected String defaultVar = null;
	protected String defaultObj = null;
	protected String defaultOnload = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		eventService = (EventService) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("eventService");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";

		boolean securityCleared = true;
		if (securedApplication) {
			String authenticatedUser = (String) req.getSession().getAttribute(AUTHENTICATED_USER);
			if (StringUtils.isEmpty(authenticatedUser)) {
				securityCleared = false;
			}
		}
		if (securityCleared) {
			String fileName = "";
			if (StringUtils.isNotEmpty(servletPath)) {
				fileName = servletPath.replace("/", "").replace("*", "");
				if (fileName.indexOf(".js") == -1) {
					fileName += ".js";
				}
			}
			String callback = req.getParameter("callback");
			String var = req.getParameter("var");
			String obj = req.getParameter("obj");
			if (StringUtils.isEmpty(callback)) {
				if (StringUtils.isEmpty(var)) {
					if (StringUtils.isEmpty(obj)) {
						if (StringUtils.isNotEmpty(defaultVar)) {
							var = defaultVar;
						}
					}
				}
			}
			if (StringUtils.isNotEmpty(callback)) {
				response += "// " + fileName + "\n\n";
				response += callback + "(";
			} else if (StringUtils.isNotEmpty(var)) {
				response += "// " + fileName + "\n\n";
				response += "var " + var + " = ";
			} else if (StringUtils.isNotEmpty(obj)) {
				response += "// " + fileName + "\n\n";
				response += obj + " = ";
			} else if (StringUtils.isNotEmpty(defaultObj)) {
				response += "// " + fileName + "\n\n";
				response += defaultObj + " = ";
			}
			response += processRequest(req, res);
			if (StringUtils.isNotEmpty(callback)) {
				response += ");";
			} else if (StringUtils.isNotEmpty(var) || StringUtils.isNotEmpty(obj) || StringUtils.isNotEmpty(defaultObj)) {
				response += ";\n";
				String onload = req.getParameter("onload");
				if (StringUtils.isEmpty(onload)) {
					if (StringUtils.isNotEmpty(defaultOnload)) {
						onload = defaultOnload;
					}
				}
				if (StringUtils.isNotEmpty(onload)) {
					response += "\n" + onload + "();\n";
				}
			}
		} else {
			response = "base.unauthorizedAccessResponse();";
		}
		res.setCharacterEncoding("UTF-8");
		res.setContentType("application/javascript;charset=UTF-8");
		res.getWriter().write(response);
    }

	/**
	 * <p>The Servlet "doPost" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The POST method is not allowed for this URL");
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 * @throws IOException 
	 */
	protected abstract String processRequest(HttpServletRequest req, HttpServletResponse res) throws IOException;

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the id present in the URL, if any
	 */
	protected String getIdFromUrl(HttpServletRequest req) {
		String id = null;

		String basePath = req.getContextPath() + servletPath;
		if (req.getRequestURI().length() > basePath.length()) {
			id = req.getRequestURI().substring(basePath.length());
		}

		return id;
	}

	/**
	 * <p>Sends the HTTP error code and message, and logs the code and message if enabled.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param errorCode the error code to send
	 * @param errorMessage the error message to send
	 */
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage) throws IOException {
		sendError(req, res, errorCode, errorMessage, null);
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
	protected void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, Throwable throwable) throws IOException {
		// log message
		if (throwable != null) {
			log.error("Sending error " + errorCode + "; message=" + errorMessage, throwable);
		} else if (log.isDebugEnabled()) {
			log.debug("Sending error " + errorCode + "; message=" + errorMessage);
		}

		// send error
		res.setContentType("text/plain");
		res.sendError(errorCode, errorMessage);
	}
}
