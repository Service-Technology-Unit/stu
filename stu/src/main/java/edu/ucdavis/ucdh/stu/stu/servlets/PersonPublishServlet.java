package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.stu.utils.PersonUpdatePoster;

/**
 * <p>This servlet publishes the data for a single person in the HS Person Repository.</p>
 */
public class PersonPublishServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private PersonUpdatePoster personUpdatePoster = null;
	private Log log = LogFactory.getLog(getClass());

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		personUpdatePoster = (PersonUpdatePoster) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("personUpdatePoster");
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
		String id = getIdFromUrl(req, "/publishperson/");
		String action = "change";
		if (id.indexOf("/") != -1) {
			String[] parts = id.split("/");
			id = parts[0];
			action = parts[1];
		}
		personUpdatePoster.publishPersonInfo(id, action);
		res.setCharacterEncoding("UTF-8");
		res.setContentType("text/plain;charset=UTF-8");
		res.getWriter().write("Publication initiated for " + id + "; action: " + action);
    }

	/**
	 * <p>The Servlet "doPost" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The POST method is not allowed for this URL");
    }

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param servletPath the path to the servlet
	 * @return the id present in the URL, if any
	 */
	private String getIdFromUrl(HttpServletRequest req, String servletPath) {
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
	private void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage) throws IOException {
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
	private void sendError(HttpServletRequest req, HttpServletResponse res, int errorCode, String errorMessage, Throwable throwable) throws IOException {
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
