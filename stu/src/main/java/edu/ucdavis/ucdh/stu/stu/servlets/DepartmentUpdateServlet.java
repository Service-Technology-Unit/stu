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
 * <p>This servlet processes a single add/change/delete action for UCDHS departments.</p>
 */
public class DepartmentUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private DataSource dataSource = null;
	private Log log = LogFactory.getLog(getClass());

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("dataSource");
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
		String deptId = req.getParameter("deptId");
		if (log.isDebugEnabled()) {
			log.debug("Processing new update - Publisher: " + publisherId + "; Subscription: " + subscriptionId + "; Request: " + requestId + "; Job: " + jobId + "; Department: " + deptId);
		}
		if (StringUtils.isNotEmpty(action)) {
			if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete")) {
				if (StringUtils.isNotEmpty(deptId)) {
					Map<String,String> department = fetchDepartment(deptId);
					if (department != null) {
						if (action.equalsIgnoreCase("delete")) {
							if ("t".equalsIgnoreCase(department.get("active"))) {
								response = deactivateDepartment(deptId);
							} else {
								response = "1;No action taken -- department already inactive";
							}
						} else {
							if ("t".equalsIgnoreCase(department.get("active")) && department.get("name").equals(req.getParameter("name")) && department.get("managerPpsid").equals(req.getParameter("managerPpsid"))) {
								response = "1;No action taken -- no changes detected";
							} else {
								response = updateDepartment(req);
							}
						}
					} else {
						if (action.equalsIgnoreCase("delete")) {
							response = "1;No action taken -- department not on file";
						} else {
							response = insertDepartment(req);
						}
					}
				} else {
					response = "2;Error - Required parameter \"deptId\" has no value";
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
	 * <p>Fetches the department data from the database.</p>
	 *
	 * @param deptId the ID of the department
	 * @return the department data
	 */
	private Map<String,String> fetchDepartment(String deptId) {
		Map<String,String> department = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching department for deptId " + deptId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = "SELECT d.DEPT_ID, d.DEPT_NAME, c.USER_ID, d.UCD_DEPT_CODE, d.UCD_ACTIVE FROM DEPTM1 d LEFT OUTER JOIN CONTCTSM1 c ON c.CONTACT_NAME=d.UCD_DEPT_MGR WHERE d.DEPT_ID=?";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, deptId);
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Department " + deptId + " found");
				}
				department = new HashMap<String,String>();
				department.put("deptId", deptId);
				department.put("name", "");
				department.put("managerPpsid", "");
				department.put("ppsDeptId", deptId);
				department.put("active", "f");
				if (StringUtils.isNotEmpty(rs.getString("DEPT_NAME"))) {
					department.put("name", rs.getString("DEPT_NAME"));
				}
				if (StringUtils.isNotEmpty(rs.getString("USER_ID"))) {
					department.put("managerPpsid", rs.getString("USER_ID"));
				}
				if (StringUtils.isNotEmpty(rs.getString("UCD_DEPT_CODE"))) {
					department.put("ppsDeptId", rs.getString("UCD_DEPT_CODE"));
				}
				if ("t".equalsIgnoreCase(rs.getString("UCD_ACTIVE"))) {
					department.put("active", "t");
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Department " + deptId + " not found");
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching department " + deptId + ": " + e.getMessage(), e);
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
			log.debug("Returning department: " + department);
		}

		return department;
	}

	/**
	 * <p>Inserts a new department using the data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the response string
	 */
	private String insertDepartment(HttpServletRequest req) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Inserting department " + req.getParameter("deptId"));
		}
		String deptId = req.getParameter("deptId");
		String name = req.getParameter("name");
		String managerPpsid = req.getParameter("managerPpsid");
		String ppsDeptId = req.getParameter("ppsDeptId");
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = "INSERT INTO DEPTM1 (DEPT_ID, DEPT, LAST_UPDATE, UPDATED_BY, SYSMODTIME, EVENT_UPDATED, SYSMODCOUNT, SYSMODUSER, COMPANY, DEPT_STRUCTURE, DEPT_FULL_NAME, [LEVEL], DEPT_NAME, DELFLAG, SLVL, UCD_ACTIVE, UCD_DEPT_MGR, UCD_DEPT_CODE) values(?, ?, GETDATE(), ?, GETDATE(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT MAX(CONTACT_NAME) FROM CONTCTSM1 WHERE ACTIVE='t' AND USER_ID=?), ?)";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, deptId);
			ps.setString(2, "UCDHS/" + deptId + " - " + name);
			ps.setString(3, "Up2Date");
			ps.setString(4, "f");
			ps.setInt(5, 0);
			ps.setString(6, "Up2Date");
			ps.setString(7, "UCDHS");
			ps.setString(8, name);
			ps.setString(9, "UCDHS/" + name);
			ps.setInt(10, 0);
			ps.setString(11, name);
			ps.setInt(12, 1);
			ps.setInt(13, 2);
			ps.setString(14, "t");
			ps.setString(15, managerPpsid);
			ps.setString(16, ppsDeptId);
			if (ps.executeUpdate() > 0) {
				response = "0;Department inserted";
				if (log.isDebugEnabled()) {
					log.debug("Department successfully inserted");
				}
			} else {
				response = "2;Unable to insert department";
				log.error("Unable to insert department " + deptId);
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to insert department: " + e.toString();
			log.error("Exception encountered while attempting to insert department " + deptId + "; " + e.getMessage(), e);
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
	 * <p>Updates the department data from the input parameters.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the response string
	 */
	private String updateDepartment(HttpServletRequest req) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Updating department " + req.getParameter("deptId"));
		}
		String deptId = req.getParameter("deptId");
		String name = req.getParameter("name");
		String managerPpsid = req.getParameter("managerPpsid");
		String ppsDeptId = req.getParameter("ppsDeptId");
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = "UPDATE DEPTM1 SET DEPT=?, LAST_UPDATE=GETDATE(), UPDATED_BY='Up2Date', SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='Up2Date', DEPT_STRUCTURE=?, DEPT_FULL_NAME=?, DEPT_NAME=?, UCD_ACTIVE='t', UCD_DEPT_MGR=(SELECT MAX(CONTACT_NAME) FROM CONTCTSM1 WHERE ACTIVE='t' AND USER_ID=?), UCD_DEPT_CODE=? WHERE DEPT_ID=?";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, "UCDHS/" + deptId + " - " + name);
			ps.setString(2, name);
			ps.setString(3, "UCDHS/" + name);
			ps.setString(4, name);
			ps.setString(5, managerPpsid);
			ps.setString(6, ppsDeptId);
			ps.setString(7, deptId);
			if (ps.executeUpdate() > 0) {
				response = "0;Department updated";
				if (log.isDebugEnabled()) {
					log.debug("Department successfully updated");
				}
			} else {
				response = "2;Unable to update department";
				log.error("Unable to update department " + deptId);
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to update department: " + e.toString();
			log.error("Exception encountered while attempting to update department " + deptId + "; " + e.getMessage(), e);
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
	 * <p>Deactivates the department.</p>
	 *
	 * @param deptId the department id
	 * @return the response string
	 */
	private String deactivateDepartment(String deptId) {
		String response = "";

		if (log.isDebugEnabled()) {
			log.debug("Deactivating department " + deptId);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = "UPDATE DEPTM1 SET UCD_ACTIVE='f', LAST_UPDATE=GETDATE(), UPDATED_BY='Up2Date', SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='Up2Date' WHERE DEPT_ID=?";
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			ps.setString(1, deptId);
			if (ps.executeUpdate() > 0) {
				response = "0;Department deactivated";
				if (log.isDebugEnabled()) {
					log.debug("Department successfully deactivated");
				}
			} else {
				response = "2;Unable to deactivate department";
				log.error("Unable to deactivate department " + deptId);
			}
		} catch (Exception e) {
			response = "2;Exception encountered while attempting to deactivate department: " + e.toString();
			log.error("Exception encountered while attempting to deactivate department " + deptId + "; " + e.getMessage(), e);
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
