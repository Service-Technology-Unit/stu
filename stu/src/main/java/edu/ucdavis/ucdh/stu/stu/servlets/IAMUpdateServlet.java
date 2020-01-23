package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * <p>This servlet processes a single add/change/delete action for ITAC persons.</p>
 */
public class IAMUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private DataSource dataSource = null;
	private Log log = LogFactory.getLog(getClass());

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		Hashtable<String,String> parms = new Hashtable<String,String>();
		try {
			Context ctx = new InitialContext(parms);
			dataSource = (DataSource) ctx.lookup("java:comp/env/jdbc/masterdata");
		} catch (Exception e) {
			log.error("Exception encountered attempting to obtain datasource jdbc/masterdata", e);
		}
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
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The GET method is not allowed for this URL");
    }

	/**
	 * <p>The Servlet "doPost" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";

		String publisherId = req.getParameter("_pid");
		String requestId = req.getParameter("_rid");
		String subscriptionId = req.getParameter("_sid");
		String jobId = req.getParameter("_jid");
		String action = req.getParameter("_action");
		String personId = req.getParameter("id");
		if (log.isDebugEnabled()) {
			log.debug("Processing new update - Publisher: " + publisherId + "; Subscription: " + subscriptionId + "; Request: " + requestId + "; Job: " + jobId + "; Person: " + personId);
		}
		if (StringUtils.isNotEmpty(action)) {
			if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete") || action.equalsIgnoreCase("force")) {
				if (StringUtils.isNotEmpty(personId)) {
					Map<String,String> newPerson = buildPersonFromRequest(req);
					Map<String,String> oldPerson = fetchPerson(personId);
					if (oldPerson != null) {
						if (!action.equalsIgnoreCase("force") && personUnchanged(req, newPerson, oldPerson)) {
							response = "1;No action taken -- no changes detected";
						} else {
							response = updatePerson(req, newPerson, oldPerson);
						}
					} else {
						if (action.equalsIgnoreCase("delete")) {
							response = "1;No action taken -- person not on file";
						} else {
							response = insertPerson(req, newPerson);
						}
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
    }

	/**
	 * <p>Fetches the person data from the database.</p>
	 *
	 * @param personId the ID of the person
	 * @return the person data
	 */
	private Map<String,String> fetchPerson(String personId) {
		Map<String,String> person = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching person for personId " + personId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			String sql = "SELECT IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE FROM IAM WHERE IAMID=?";
			ps = conn.prepareStatement(sql);
			ps.setString(1, personId);
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Person " + personId + " found");
				}
				person = new HashMap<String,String>();
				person.put("IAMID", rs.getString("IAMID"));
				person.put("MOTHRAID", rs.getString("MOTHRAID"));
				person.put("PPSID", rs.getString("PPSID"));
				person.put("BANNER_PIDM", rs.getString("BANNER_PIDM"));
				person.put("STUDENTID", rs.getString("STUDENTID"));
				person.put("EXTERNALID", rs.getString("EXTERNALID"));
				person.put("FIRST_NAME", rs.getString("FIRST_NAME"));
				person.put("LAST_NAME", rs.getString("LAST_NAME"));
				person.put("MIDDLE_NAME", rs.getString("MIDDLE_NAME"));
				person.put("PRI_HSAD_ACCOUNT", rs.getString("PRI_HSAD_ACCOUNT"));
				person.put("KERB_ACCOUNT", rs.getString("KERB_ACCOUNT"));
				person.put("PRI_UCDHS_DEPT_CODE", rs.getString("PRI_UCDHS_DEPT_CODE"));
				person.put("PRI_UCDHS_DEPT_NAME", rs.getString("PRI_UCDHS_DEPT_NAME"));
				person.put("EMAIL", rs.getString("EMAIL"));
				person.put("IS_EMPLOYEE", rs.getString("IS_EMPLOYEE"));
				person.put("IS_STUDENT", rs.getString("IS_STUDENT"));
				person.put("IS_EXTERNAL", rs.getString("IS_EXTERNAL"));
				person.put("IS_PREVIOUS_HS_EMPLOYEE", rs.getString("IS_PREVIOUS_HS_EMPLOYEE"));
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Person " + personId + " not found");
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching person " + personId + ": " + e.getMessage(), e);
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
		if (log.isDebugEnabled()) {
			log.debug("Returning person: " + person);
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
	private boolean personUnchanged(HttpServletRequest req, Map<String,String> newPerson, Map<String,String> oldPerson) {
		boolean unchanged = false;

		if (isEqual(oldPerson, newPerson, "MOTHRAID") &&
				isEqual(oldPerson, newPerson, "PPSID") &&
				isEqual(oldPerson, newPerson, "BANNER_PIDM") &&
				isEqual(oldPerson, newPerson, "STUDENTID") &&
				isEqual(oldPerson, newPerson, "EXTERNAID") &&
				isEqual(oldPerson, newPerson, "FIRST_NAME") &&
				isEqual(oldPerson, newPerson, "LAST_NAME") &&
				isEqual(oldPerson, newPerson, "MIDDLE_NAME") &&
				isEqual(oldPerson, newPerson, "PRI_HSAD_ACCOUNT") &&
				isEqual(oldPerson, newPerson, "KERB_ACCOUNT") &&
				isEqual(oldPerson, newPerson, "PRI_UCDHS_DEPT_CODE") &&
				isEqual(oldPerson, newPerson, "PRI_UCDHS_DEPT_NAME") &&
				isEqual(oldPerson, newPerson, "EMAIL") &&
				isEqual(oldPerson, newPerson, "IS_EMPLOYEE") &&
				isEqual(oldPerson, newPerson, "IS_STUDENT") &&
				isEqual(oldPerson, newPerson, "IS_EXTERNAL") &&
				isEqual(oldPerson, newPerson, "IS_PREVIOUS_HS_EMPLOYEE")) {
			unchanged = true;
		}

		return unchanged;
	}

	/**
	 * <p>Inserts a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the response string
	 */
	private String insertPerson(HttpServletRequest req, Map<String,String> person) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Inserting person " + req.getParameter("id"));
		}
		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = "INSERT INTO IAM (IAMID, MOTHRAID, PPSID, BANNER_PIDM, STUDENTID, EXTERNALID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, PRI_HSAD_ACCOUNT, KERB_ACCOUNT, PRI_UCDHS_DEPT_CODE, PRI_UCDHS_DEPT_NAME, EMAIL, IS_EMPLOYEE, IS_STUDENT, IS_EXTERNAL, IS_PREVIOUS_HS_EMPLOYEE, SYSMODCOUNT, SYSMODUSER, SYSMODTIME, SYSMODADDR) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'up2date', getdate(), ?)";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, person.get("IAMID"));
			ps.setString(2, person.get("MOTHRAID"));
			ps.setString(3, person.get("PPSID"));
			ps.setString(4, person.get("BANNER_PIDM"));
			ps.setString(5, person.get("STUDENTID"));
			ps.setString(6, person.get("EXTERNALID"));
			ps.setString(7, person.get("FIRST_NAME"));
			ps.setString(8, person.get("LAST_NAME"));
			ps.setString(9, person.get("MIDDLE_NAME"));
			ps.setString(10, person.get("PRI_HSAD_ACCOUNT"));
			ps.setString(11, person.get("KERB_ACCOUNT"));
			ps.setString(12, person.get("PRI_UCDHS_DEPT_CODE"));
			ps.setString(13, person.get("PRI_UCDHS_DEPT_NAME"));
			ps.setString(14, person.get("EMAIL"));
			ps.setString(15, person.get("IS_EMPLOYEE"));
			ps.setString(16, person.get("IS_STUDENT"));
			ps.setString(17, person.get("IS_EXTERNAL"));
			ps.setString(18, person.get("IS_PREVIOUS_HS_EMPLOYEE"));
			ps.setString(19, remoteAddr);
			if (ps.executeUpdate() > 0) {
				if (log.isDebugEnabled()) {
					log.debug("Person successfully inserted");
				}
				response = "0;Person inserted";
			} else {
				response = "2;Unable to insert person";
				log.error("Unable to insert person " + person.get("IAMID"));
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to insert person: " + e.toString();
			log.error("Exception encountered while attempting to insert person " + person.get("IAMID") + "; " + e.getMessage(), e);
		} finally {
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
 
		return response;
	}

	/**
	 * <p>Updates the person data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the response string
	 */
	private String updatePerson(HttpServletRequest req, Map<String,String> newPerson, Map<String,String> oldPerson) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Updating person " + req.getParameter("id"));
		}
		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = "UPDATE IAM SET MOTHRAID=?, PPSID=?, BANNER_PIDM=?, STUDENTID=?, EXTERNALID=?, FIRST_NAME=?, LAST_NAME=?, MIDDLE_NAME=?, PRI_HSAD_ACCOUNT=?, KERB_ACCOUNT=?, PRI_UCDHS_DEPT_CODE=?, PRI_UCDHS_DEPT_NAME=?, EMAIL=?, IS_EMPLOYEE=?, IS_STUDENT=?, IS_EXTERNAL=?, IS_PREVIOUS_HS_EMPLOYEE=?, SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER=?, SYSMODTIME=getdate(), SYSMODADDR=? WHERE IAMID=?";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, newPerson.get("MOTHRAID"));
			ps.setString(2, newPerson.get("PPSID"));
			ps.setString(3, newPerson.get("BANNER_PIDM"));
			ps.setString(4, newPerson.get("STUDENTID"));
			ps.setString(5, newPerson.get("EXTERNALID"));
			ps.setString(6, newPerson.get("FIRST_NAME"));
			ps.setString(7, newPerson.get("LAST_NAME"));
			ps.setString(8, newPerson.get("MIDDLE_NAME"));
			ps.setString(9, newPerson.get("PRI_HSAD_ACCOUNT"));
			ps.setString(10, newPerson.get("KERB_ACCOUNT"));
			ps.setString(11, newPerson.get("PRI_UCDHS_DEPT_CODE"));
			ps.setString(12, newPerson.get("PRI_UCDHS_DEPT_NAME"));
			ps.setString(13, newPerson.get("EMAIL"));
			ps.setString(14, newPerson.get("IS_EMPLOYEE"));
			ps.setString(15, newPerson.get("IS_STUDENT"));
			ps.setString(16, newPerson.get("IS_EXTERNAL"));
			ps.setString(17, newPerson.get("IS_PREVIOUS_HS_EMPLOYEE"));
			ps.setString(18, "up2date");
			ps.setString(19, remoteAddr);
			ps.setString(20, newPerson.get("IAMID"));
			if (ps.executeUpdate() > 0) {
				if (log.isDebugEnabled()) {
					log.debug("Person successfully updated");
				}
				response = "0;Person updated";
			} else {
				response = "2;Unable to update person";
				log.error("Unable to update person " + req.getParameter("id"));
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to update person: " + e.toString();
			log.error("Exception encountered while attempting to update person " + req.getParameter("id") + "; " + e.getMessage(), e);
		} finally {
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

		return response;
	}

	/**
	 * <p>Builds a new person using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the person data from the incoming request
	 */
	private Map<String,String> buildPersonFromRequest(HttpServletRequest req) {
		Map<String,String> person = new HashMap<String,String>();

		person.put("IAMID", req.getParameter("id"));
		person.put("MOTHRAID", req.getParameter("mothraId"));
		person.put("PPSID", req.getParameter("ppsId"));
		person.put("BANNER_PIDM", req.getParameter("bannerId"));
		person.put("STUDENTID", req.getParameter("studentId"));
		person.put("EXTERNALID", req.getParameter("externalId"));
		person.put("FIRST_NAME", req.getParameter("firstName"));
		person.put("LAST_NAME", req.getParameter("lastName"));
		person.put("MIDDLE_NAME", req.getParameter("middleName"));
		person.put("PRI_HSAD_ACCOUNT", req.getParameter("adId"));
		person.put("KERB_ACCOUNT", req.getParameter("kerberosId"));
		person.put("PRI_UCDHS_DEPT_CODE", req.getParameter("deptId"));
		person.put("PRI_UCDHS_DEPT_NAME", req.getParameter("deptName"));
		person.put("EMAIL", req.getParameter("email"));
		person.put("IS_EMPLOYEE", req.getParameter("isEmployee"));
		person.put("IS_STUDENT", req.getParameter("isStudent"));
		person.put("IS_EXTERNAL", req.getParameter("isExternal"));
		person.put("IS_PREVIOUS_HS_EMPLOYEE", req.getParameter("isPrevEmployee"));

		return person;
	}

	private boolean isEqual(Map<String,String> p1, Map<String,String> p2, String key) {
		boolean eq = true;

		String s1 = p1.get(key);
		String s2 = p2.get(key);
		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
			} else {
				eq = false;
			}
		} else {
			if (StringUtils.isNotEmpty(s2)) {
				eq = false;
			}
		}

		return eq;
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
