package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * <p>This servlet updates the CardKey cardholder database with data from the UCDH Person Repository.</p>
 */
public class CardHolderUpdateServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private Log log = LogFactory.getLog(getClass());
	private int iamIdIndex = -1;
	private String authorizedIpList = null;
	private List<String> authorizedIp = new ArrayList<String>();
	private DataSource dataSource = null;
	private DataSource smDataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		authorizedIpList = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("authorizedIpList");
		if (StringUtils.isNotEmpty(authorizedIpList)) {
			String[] ipArray = authorizedIpList.split(",");
			for (int i=0; i<ipArray.length; i++) {
				authorizedIp.add(ipArray[i].trim());
			}
		}
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("cardKeyDataSource");
		smDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("dataSource");
		iamIdIndex = fetchIamIdIndex();
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
		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		if (authorizedIp.contains(remoteAddr)) {
			String response = "";
			String action = req.getParameter("_action");
			String iamId = req.getParameter("id");
			if (log.isDebugEnabled()) {
				log.debug("Processing new update - IAM ID: " + iamId + "; Action: " + action);
			}
			if (StringUtils.isNotEmpty(action)) {
				if (action.equalsIgnoreCase("add") || action.equalsIgnoreCase("change") || action.equalsIgnoreCase("delete") || action.equalsIgnoreCase("force")) {
					if (StringUtils.isNotEmpty(iamId)) {
						Map<String,String> newPerson = buildPersonFromRequest(req);
						Map<String,String> oldPerson = fetchCardholder(iamId, req.getParameter("ucPathId"));
						if (oldPerson != null) {
							if (!action.equalsIgnoreCase("force") && personUnchanged(req, newPerson, oldPerson)) {
								response = "1;No action taken -- no changes detected";
							} else {
								response = updateCardholder(req, res, newPerson, oldPerson);
							}
						} else {
							if (action.equalsIgnoreCase("delete")) {
								response = "1;No action taken -- person not on file";
							} else {
								if ("INACTIVE".equalsIgnoreCase(newPerson.get("HR_NOTES"))) {
									response = "1;No action taken -- INACTIVE persons are not inserted";
								} else {
									response = updateCardholder(req, res, newPerson, null);
								}
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
		} else {
			sendError(req, res, HttpServletResponse.SC_FORBIDDEN, remoteAddr + " is not authorized to access this service");
		}
	}

	/**
	 * <p>Returns the card holder data on file in the card holder database, if present.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @param ucPathId the UCPath ID of the person
	 * @return the cardholder's data from the card key system
	 * @throws IOException 
	 */
	private Map<String,String> fetchCardholder(String iamId, String ucPathId) throws IOException {
		Map<String,String> person = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching card key database for IAM ID #" + iamId + " ...");
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String cardholderId = getCardholderId(iamId);
		try {
			con = dataSource.getConnection();
			if (StringUtils.isEmpty(cardholderId)) {
				if (StringUtils.isNotEmpty(ucPathId)) {
					ps = con.prepareStatement("SELECT c_id FROM cardholder WHERE c_nick_name=?");
					ps.setString(1, ucPathId);
					rs = ps.executeQuery();
					if (rs.next()) {
						cardholderId = rs.getString(1);
						if (log.isDebugEnabled()) {
							log.debug("Found UCPath ID #" + ucPathId + " on file with a cardholder ID of " + cardholderId);
						}
						try {
							rs.close();
						} catch (Exception e) {
							// no one cares!
						}
						try {
							ps.close();
						} catch (Exception e) {
							// no one cares!
						}
					}
				}
			}
			if (StringUtils.isNotEmpty(cardholderId)) {
				ps = con.prepareStatement("SELECT c_lname, c_fname, c_mname, c_nick_name, c_addr, c_addr1, c_addr2, c_addr3, c_suite, c_phone, c_email, c_s_timestamp, c_t_timestamp, c_sponsor_id FROM cardholder WHERE c_id=?");
				ps.setString(1, cardholderId);
				rs = ps.executeQuery();
				if (rs.next()) {
					person = new HashMap<String,String>();
					person.put("CARDHOLDER_ID", cardholderId);
					person.put("LAST_NAME", nullify(rs.getString("c_lname")));
					person.put("FIRST_NAME", nullify(rs.getString("c_fname")));
					person.put("MIDDLE_NAME", nullify(rs.getString("c_mname")));
					person.put("UC_PATH_ID", nullify(rs.getString("c_nick_name")));
					person.put("ADDRESS", nullify(rs.getString("c_addr")));
					person.put("CITY", nullify(rs.getString("c_addr1")));
					person.put("STATE", nullify(rs.getString("c_addr2")));
					person.put("ZIP", nullify(rs.getString("c_addr3")));
					person.put("ROOM", nullify(rs.getString("c_suite")));
					person.put("PHONE", nullify(rs.getString("c_phone")));
					person.put("EMAIL", nullify(rs.getString("c_email")));
					person.put("VALID_FROM", nullify(rs.getString("c_s_timestamp")));
					person.put("VALID_TO", nullify(rs.getString("c_t_timestamp")));
					person.put("SPONSOR", nullify(rs.getString("c_sponsor_id")));
					try {
						rs.close();
					} catch (Exception e) {
						// no one cares!
					}
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
					ps = con.prepareStatement("SELECT LTRIM(RTRIM(a.ug_label)) AS LABEL, LTRIM(RTRIM(b.ut_text)) AS VALUE FROM udfgen a LEFT OUTER JOIN udftext b ON b.ut_udfgen_id=a.ug_id AND b.ut_cardholder_id=? WHERE a.ug_hidefrommis=0 ORDER BY a.ug_order");
					ps.setString(1, cardholderId);
					rs = ps.executeQuery();
					while (rs.next()) {
						person.put(rs.getString("LABEL"), rs.getString("VALUE"));
					}
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred while attempting to find IAM ID #" + iamId + ": " + e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning existing cardholder: " + person);
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

		if (isEqual(oldPerson, newPerson, "ADDRESS") &&
				isEqual(oldPerson, newPerson, "ALT ID") &&
				isEqual(oldPerson, newPerson, "CITY") &&
				isEqual(oldPerson, newPerson, "EMAIL") &&
				isEqual(oldPerson, newPerson, "EXPIRATION_DATE") &&
				isEqual(oldPerson, newPerson, "FIRST_NAME") &&
				isEqual(oldPerson, newPerson, "HR_DEPT") &&
				isEqual(oldPerson, newPerson, "HR_DEPTID") &&
				isEqual(oldPerson, newPerson, "HR_TITLE") &&
				isEqual(oldPerson, newPerson, "IAM ID") &&
				isEqual(oldPerson, newPerson, "LAST_NAME") &&
				isEqual(oldPerson, newPerson, "MIDDLE_NAME") &&
				isEqual(oldPerson, newPerson, "ROOM") &&
				isEqual(oldPerson, newPerson, "TITLE 1") &&
				isEqual(oldPerson, newPerson, "TITLE 2") &&
				isEqual(oldPerson, newPerson, "TITLE 3") &&
				isEqual(oldPerson, newPerson, "UC_PATH_ID") &&
				isEqual(oldPerson, newPerson, "ZIP")) {
			unchanged = true;
		}

		return unchanged;
	}

	/**
	 * <p>Returns the response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param newPerson the new data for this person
	 * @param oldPerson the existing data for this person
	 * @return the response
	 * @throws IOException 
	 */
	private String updateCardholder(HttpServletRequest req, HttpServletResponse res, Map<String,String> newPerson, Map<String,String> oldPerson) throws IOException {
		String response = null;

		String cardholderId = null;
		if (oldPerson != null) {
			if (StringUtils.isNotEmpty(oldPerson.get("CARDHOLDER_ID"))) {
				cardholderId = oldPerson.get("CARDHOLDER_ID");
			}
			if (StringUtils.isEmpty(newPerson.get("UC_PATH_ID"))) {
				newPerson.put("UC_PATH_ID", oldPerson.get("UC_PATH_ID"));
				newPerson.put("EMP_ID", oldPerson.get("UC_PATH_ID"));
			}
			if (StringUtils.isEmpty(newPerson.get("HR_DEPTID"))) {
				newPerson.put("HR_DEPT", oldPerson.get("HR_DEPT"));
				newPerson.put("HR_DEPTID", oldPerson.get("HR_DEPTID"));
			}
		}
		if (log.isDebugEnabled()) {
			if (StringUtils.isNotEmpty(cardholderId)) {
				log.debug("Updating existing cardholder data for " + newPerson.get("FIRST_NAME") + " " + newPerson.get("LAST_NAME") + " (" + cardholderId + ")");
			} else {
				log.debug("Inserting data for new cardholder " + newPerson.get("FIRST_NAME") + " " + newPerson.get("LAST_NAME"));
			}
		}
		Connection con = null;
		PreparedStatement ps = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement("INSERT INTO Pegasys_Util.dbo.Super_User_Queue ([CardHolderID], [RequestID], [FirstName], [MiddleName], [LastName], [NickName], [Address], [City], [State], [Zip], [Room], [Phone], [Email], [StartDate], [EndDate], [SponsorID], [NAME], [FIRST], [LAST], [DEGREES], [TITLE_1], [TITLE_2], [TITLE_3], [EXPIRATION_DATE], [EMP_ID], [ALT_ID], [MEAL_CARD], [NOTES_1], [NOTES_2], [HR_TITLE], [HR_DEPT], [HR_DEPTID], [HR_NOTES], [IAM_ID], [QueueStart]) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, getdate())");
			ps.setString(1, nullify(cardholderId));
			ps.setString(2, nullify(req.getParameter("_rid")));
			ps.setString(3, nullify(newPerson.get("FIRST_NAME")));
			ps.setString(4, nullify(newPerson.get("MIDDLE_NAME")));
			ps.setString(5, nullify(newPerson.get("LAST_NAME")));
			ps.setString(6, nullify(newPerson.get("UC_PATH_ID")));
			ps.setString(7, nullify(newPerson.get("ADDRESS")));
			ps.setString(8, nullify(newPerson.get("CITY")));
			ps.setString(9, nullify(newPerson.get("STATE")));
			ps.setString(10, nullify(newPerson.get("ZIP")));
			ps.setString(11, nullify(newPerson.get("ROOM")));
			ps.setString(12, nullify(newPerson.get("PHONE")));
			String email = null;
			if (StringUtils.isNotEmpty(newPerson.get("EMAIL"))) {
				email = newPerson.get("EMAIL").toLowerCase();
			}
			ps.setString(13, email);
			ps.setString(14, nullify(newPerson.get("VALID_FROM")));
			ps.setString(15, nullify(newPerson.get("VALID_TO")));
			ps.setString(16, nullify(newPerson.get("SPONSOR")));
			ps.setString(17, nullify(newPerson.get("NAME")));
			ps.setString(18, nullify(newPerson.get("FIRST")));
			ps.setString(19, nullify(newPerson.get("LAST")));
			ps.setString(20, nullify(newPerson.get("DEGREES")));
			ps.setString(21, nullify(newPerson.get("TITLE_1")));
			ps.setString(22, nullify(newPerson.get("TITLE_2")));
			ps.setString(23, nullify(newPerson.get("TITLE_3")));
			ps.setString(24, nullify(newPerson.get("EXPIRATION_DATE")));
			ps.setString(25, nullify(newPerson.get("EMP_ID")));
			ps.setString(26, nullify(newPerson.get("ALT_ID")));
			ps.setString(27, nullify(newPerson.get("MEAL_CARD")));
			ps.setString(28, nullify(newPerson.get("NOTES_1")));
			ps.setString(29, nullify(newPerson.get("NOTES_2")));
			ps.setString(30, nullify(newPerson.get("HR_TITLE")));
			ps.setString(31, nullify(newPerson.get("HR_DEPT")));
			ps.setString(32, nullify(newPerson.get("HR_DEPTID")));
			ps.setString(33, nullify(newPerson.get("HR_NOTES")));
			ps.setString(34, nullify(newPerson.get("IAM_ID")));
			int insertCt = ps.executeUpdate();
			if (insertCt == 1) {
				response = "0;Update added to the pending work queue";
			} else {
				response = "1;Unable to add update to the pending work queue";
			}
		} catch (Exception e) {
			log.error("Exception occurred while attempting to add update to the pending work queue: " + e, e);
			response = "2;Exception occurred while attempting to add update to the pending work queue: " + e;
		} finally {
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Update response: " + response);
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

		person.put("FIRST_NAME", req.getParameter("firstName"));
		person.put("MIDDLE_NAME", req.getParameter("middleName"));
		person.put("LAST_NAME", req.getParameter("lastName"));
		person.put("UC_PATH_ID", req.getParameter("ucPathId"));
		person.put("ADDRESS", req.getParameter("address"));
		person.put("CITY", req.getParameter("city"));
		person.put("STATE", req.getParameter("state"));
		person.put("ZIP", req.getParameter("zip"));
		person.put("ROOM", req.getParameter("room"));
		person.put("PHONE", req.getParameter("phoneNumber"));
		person.put("EMAIL", req.getParameter("email"));
		person.put("VALID_FROM", req.getParameter("startDate"));
		person.put("VALID_TO", req.getParameter("endDate"));
		person.put("NAME", req.getParameter("firstName") + " " + req.getParameter("lastName"));
		person.put("FIRST", req.getParameter("firstName"));
		person.put("LAST", req.getParameter("lastName"));
		person.put("DEGREES", null);
		person.put("TITLE_1", null);
		person.put("TITLE_2", null);
		person.put("TITLE_3", null);
		person.put("EXPIRATION_DATE", null);
		if (StringUtils.isNotEmpty(req.getParameter("endDate"))) {
			person.put("EXPIRATION_DATE", "Exp: " + req.getParameter("endDate").substring(0, 10));
		}
		person.put("EMP_ID", req.getParameter("ucPathId"));
		person.put("ALT_ID", null);
		if (StringUtils.isNotEmpty(req.getParameter("studentId"))) {
			person.put("ALT_ID", "SID: " + req.getParameter("studentId"));
		}
		person.put("MEAL_CARD", null);
		person.put("NOTES_1", null);
		person.put("NOTES_2", null);
		person.put("HR_TITLE", req.getParameter("title"));
		person.put("HR_DEPT", req.getParameter("deptName"));
		person.put("HR_DEPTID", req.getParameter("deptId"));
		person.put("HR_NOTES", null);
		if (!"Y".equalsIgnoreCase(req.getParameter("isActive"))) {
			person.put("HR_NOTES", "INACTIVE");
		}
		person.put("IAM_ID", req.getParameter("id"));
		if (StringUtils.isEmpty(req.getParameter("ucPathId"))) {
			person.put("SPONSOR", getUcpathId(req.getParameter("supervisor")));
			if (StringUtils.isEmpty(person.get("SPONSOR"))) {
				if (StringUtils.isNotEmpty(person.get("HR_DEPTID"))) {
					person.put("SPONSOR", getDepartmentManager(person.get("HR_DEPTID")));
				}
			}
		} else {
			person.put("SPONSOR", null);			
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning new cardholder values: " + person);
		}

		return person;
	}

	/**
	 * <p>Returns the card holder id for the iam id passed.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @return the card holder id for the iam id passed
	 */
	private String getCardholderId(String iamId) {
		String cardholderId = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching for Card Holder ID for IAM ID #" + iamId + " ...");
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement("SELECT ut_cardholder_id FROM udftext WHERE ut_udfgen_id=28 AND ut_text=?");
			ps.setString(1, iamId);
			rs = ps.executeQuery();
			if (rs.next()) {
				cardholderId = rs.getString(1);
				if (log.isDebugEnabled()) {
					log.debug("Found IAM ID #" + iamId + " on file with a cardholder ID of " + cardholderId);
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred while attempting to find Card Holder ID for IAM ID #" + iamId + ": " + e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning cardholderId: " + cardholderId);
		}

		return cardholderId;
	}

	/**
	 * <p>Returns the ucpath id for the iam id passed.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @return the ucpath id for the iam id passed
	 */
	private String getUcpathId(String iamId) {
		String ucPathId = null;

		if (StringUtils.isNotEmpty(iamId)) {
			if (log.isDebugEnabled()) {
				log.debug("Searching for UCPath ID for IAM ID #" + iamId + " ...");
			}
			Connection con = null;
			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				con = dataSource.getConnection();
				ps = con.prepareStatement("SELECT b.c_nick_name FROM udftext a LEFT OUTER JOIN cardholder b ON b.c_id=a.ut_cardholder_id WHERE a.ut_udfgen_id=" + iamIdIndex + " AND a.ut_text=?");
				ps.setString(1, iamId);
				rs = ps.executeQuery();
				if (rs.next()) {
					ucPathId = rs.getString(1);
					if (log.isDebugEnabled()) {
						log.debug("Found IAM ID #" + iamId + " on file with a UCPath ID of " + ucPathId);
					}
				}
			} catch (Exception e) {
				log.error("Exception occurred while attempting to find UCPath ID for IAM ID #" + iamId + ": " + e, e);
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (ps != null) {
					try {
						ps.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						// no one cares!
					}
				}
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("No UCPATH ID available for empty IAM ID");
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning ucPathId: " + ucPathId);
		}

		return ucPathId;
	}

	/**
	 * <p>Returns the pps id for manager of the specified department.</p>
	 *
	 * @param deptId the id of the department
	 * @return the pps id for manager of the specified department
	 */
	private String getDepartmentManager(String deptId) {
		String ppsId = null;

		if (log.isDebugEnabled()) {
			log.debug("Searching for PPS ID for Department ID " + deptId + " ...");
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = smDataSource.getConnection();
			ps = con.prepareStatement("SELECT b.USER_ID FROM DEPTM1 a LEFT OUTER JOIN CONTCTSM1 b ON b.CONTACT_NAME=a.UCD_DEPT_MGR AND b.ACTIVE='t' WHERE a.UCD_ACTIVE='t' AND a.DEPT_ID=?");
			ps.setString(1, deptId);
			rs = ps.executeQuery();
			if (rs.next()) {
				ppsId = rs.getString(1);
				if (log.isDebugEnabled()) {
					log.debug("Found Department ID " + deptId + " on file with a Manager PPS ID of " + ppsId);
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred while attempting to find PPS ID for Department ID " + deptId + ": " + e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning department manager ppsId: " + ppsId);
		}

		return ppsId;
	}

	/**
	 * <p>Returns the card holder id for the iam id passed.</p>
	 *
	 * @param iamId the IAM ID of the person
	 * @return the card holder id for the iam id passed
	 */
	private int fetchIamIdIndex() {
		int inx = -1;

		if (log.isDebugEnabled()) {
			log.debug("Searching for index value for IAM UDF field");
		}
		Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			con = dataSource.getConnection();
			ps = con.prepareStatement("SELECT ug_id FROM udfgen WHERE ug_label='IAM_ID'");
			rs = ps.executeQuery();
			if (rs.next()) {
				inx = rs.getInt(1);
				if (log.isDebugEnabled()) {
					log.debug("Found index value for IAM UDF field: " + inx);
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred while attempting to find index value for IAM UDF field: " + e, e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
			if (con != null) {
				try {
					con.close();
				} catch (Exception e) {
					// no one cares!
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Returning  index value for IAM UDF field: " + inx);
		}

		return inx;
	}

	private boolean isEqual(Map<String,String> p1, Map<String,String> p2, String key) {
		boolean eq = true;

		String s1 = p1.get(key);
		String s2 = p2.get(key);
		if (StringUtils.isNotEmpty(s1)) {
			if (StringUtils.isNotEmpty(s2)) {
				eq = s1.equals(s2);
			}
		} else {
			if (StringUtils.isNotEmpty(s2)) {
				eq = false;
			}
		}

		return eq;
	}

	private String nullify(String value) {
		String response = null;

		if (StringUtils.isNotEmpty(value)) {
			response = value.trim();
			if ("null".equalsIgnoreCase(response)) {
				response = null;
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
		res.setContentType("text/plain;charset=UTF-8");
		res.sendError(errorCode, errorMessage);
	}
}
