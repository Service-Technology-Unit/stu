package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * <p>This servlet produces a vcard for a specific contact database entry.</p>
 */
public class VcardServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private Log log = LogFactory.getLog(getClass());
	DataSource dataSource = null;
	Map<String,String> buildInfo = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	@SuppressWarnings("unchecked")
	public void init() throws ServletException {
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("dataSource");
		buildInfo = (Map<String,String>) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("buildInfo");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";

		String id = getIdFromUrl(req);
		if (StringUtils.isNotEmpty(id)) {
			Map<String,String> contact = getContact(id);
			if (contact != null) {
				response = formatVcard(req, contact);
			}
		}
		if (StringUtils.isNotEmpty(response)) {
			res.setCharacterEncoding("UTF-8");
			res.setContentType("text/directory;charset=UTF-8");
			res.getWriter().write(response);
		} else {
			sendError(req, res, HttpServletResponse.SC_NOT_FOUND, "The requested vcard is not on file");
		}
    }

	/**
	 * <p>The Servlet "doPost" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The POST method is not allowed for this URL");
	}

	/**
	 * <p>Returns the requested contact information.</p>
	 *
	 * @param id the contact id
	 * @return the requested contact information
	 */
	private Map<String,String> getContact(String id) {
		Map<String,String> contact = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching contact details for contact " + id + " ...");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement("SELECT A.CONTACT_NAME AS ID, A.LAST_NAME, A.FIRST_NAME, A.TITLE, A.DEPT, A.LOCATION_CODE, B.ADDRESS, B.CITY, B.STATE, B.ZIP, B.LATITUDE, B.LONGITUDE, A.CONTACT_PHONE, A.PORTABLE_PHONE, A.BEEPER_PHONE, A.EMAIL FROM CONTCTSM1 AS A LEFT OUTER JOIN LOCM1 AS B ON B.LOCATION_CODE=A.LOCATION_CODE WHERE A.CONTACT_NAME=?");
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				contact = new HashMap<String,String>();
				contact.put("id", escape(rs.getString("ID").toLowerCase()));
				contact.put("lastName", escape(rs.getString("LAST_NAME")));
				contact.put("firstName", escape(rs.getString("FIRST_NAME")));
				contact.put("title", escape(rs.getString("TITLE")));
				contact.put("department", escape(rs.getString("DEPT")));
				contact.put("address", escape(rs.getString("ADDRESS")));
				contact.put("city", escape(rs.getString("CITY")));
				contact.put("state", escape(rs.getString("STATE")));
				contact.put("zip", escape(rs.getString("ZIP")));
				contact.put("email", escape(rs.getString("EMAIL")));
				contact.put("phoneNr", escape(rs.getString("CONTACT_PHONE")));
				contact.put("cellNr", escape(rs.getString("PORTABLE_PHONE")));
				contact.put("pagerNr", escape(rs.getString("BEEPER_PHONE")));
				contact.put("latitude", escape(rs.getString("LATITUDE")));
				contact.put("longitude", escape(rs.getString("LONGITUDE")));
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing assignmentGroup data: " + e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					//
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (Exception e) {
					//
				}
			}
		}

		return contact;
	}

	/**
	 * <p>Formats the vCard.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param contact the contact information
	 * @return the formatted vCard
	 */
	private String formatVcard(HttpServletRequest req, Map<String,String> contact) {
		String vcard = "BEGIN:VCARD\n";

		vcard += "PRODID:-//ucdhs.ucdavis.edu//" + buildInfo.get("artifactId") + " " + buildInfo.get("version") + " (" + buildInfo.get("timestamp") + ")//EN\n";
		vcard += "SOURCE:http://" + req.getServerName() + req.getRequestURI() + "\n";
		vcard += "NAME:" + escape(contact.get("firstName")) + " " + escape(contact.get("lastName")) + "\n";
		vcard += "VERSION:3.0\n";
		vcard += "N;CHARSET=UTF-8:" + escape(contact.get("lastName")) + ";" + escape(contact.get("firstName")) + ";;;\n";
		vcard += "ORG;CHARSET=UTF-8:UC Davis Health System;" + escape(contact.get("department")) + "\n";
		vcard += "FN;CHARSET=UTF-8:" + escape(contact.get("firstName")) + " " + escape(contact.get("lastName")) + "\n";
		if (StringUtils.isNotEmpty(contact.get("title"))) {
			vcard += "TITLE;CHARSET=UTF-8:" + escape(contact.get("title")) + "\n";
		}
		vcard += "UID:\n";
		if (StringUtils.isNotEmpty(contact.get("email"))) {
			vcard += "EMAIL;TYPE=PREF,WORK,INTERNET;CHARSET=UTF-8:" + escape(contact.get("email")) + "\n";
		}
		if (StringUtils.isNotEmpty(contact.get("address"))) {
			vcard += "ADR;TYPE=WORK;CHARSET=UTF-8:;;" + escape(contact.get("address")) + ";" + escape(contact.get("city")) + ";" + escape(contact.get("state")) + ";" + escape(contact.get("zip")) + ";\n";
		}
		if (StringUtils.isNotEmpty(contact.get("phoneNr"))) {
			vcard += "TEL;TYPE=PREF,WORK,VOICE;CHARSET=UTF-8:" + escape(contact.get("phoneNr")) + "\n";
		}
		if (StringUtils.isNotEmpty(contact.get("cellNr"))) {
			vcard += "TEL;TYPE=WORK,VOICE,CELL;CHARSET=UTF-8:" + escape(contact.get("cellNr")) + "\n";
		}
		if (StringUtils.isNotEmpty(contact.get("pagerNr"))) {
			vcard += "TEL;TYPE=WORK,PAGER;CHARSET=UTF-8:" + escape(contact.get("pagerNr")) + "\n";
		}
		if (StringUtils.isNotEmpty(contact.get("latitude"))) {
			vcard += "GEO:" + escape(contact.get("latitude")) + ";" + escape(contact.get("longitude")) + "\n";
		}
		vcard += "NICKNAME;CHARSET=UTF-8:" + escape(contact.get("firstName")) + " " + escape(contact.get("lastName")) + "\n";
		vcard += "END:VCARD\n";

		return vcard;
	}

	/**
	 * <p>Converts nulls to empty string and escapes quotes.</p>
	 *
	 * @param value to string to escape
	 * @return the escaped string
	 */
	private String escape(String value) {
		String string = "";

		if (StringUtils.isNotEmpty(value)) {
			string = value;
		}

		return string;
	}

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the id present in the URL, if any
	 */
	private String getIdFromUrl(HttpServletRequest req) {
		String id = "";

		String basePath = req.getContextPath() + "/vcard/";
		if (req.getRequestURI().length() > basePath.length()) {
			id = req.getRequestURI().substring(basePath.length());
			if (id.indexOf(".vcf") != -1) {
				id = id.substring(0, id.indexOf(".vcf"));
			}
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
