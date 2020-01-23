package edu.ucdavis.ucdh.stu.stu.servlets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * <p>This servlet produces the Javascript for the building list.</p>
 */
public class BuildingServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	DataSource dataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/building.js";
		defaultVar = "building";
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("dataSource");
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 */
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		String response = "";

		if ("update".equals(req.getParameter("mode"))) {
			response = processUpdateRequest(req);
		} else {
			if (StringUtils.isNotEmpty(req.getParameter("id"))) {
				response = processSingleRequest(req.getParameter("id"));
			} else {
				response = processFullListRequest();
			}
		}

		return response;
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the Javascript response
	 */
	private String processUpdateRequest(HttpServletRequest req) {
		String string = "false";

		String authenticatedUser = (String) req.getSession().getAttribute(AUTHENTICATED_USER);
		if (StringUtils.isNotEmpty(authenticatedUser)) {
			if (log.isDebugEnabled()) {
				log.debug("Updating building information ...");
			}
			Connection conn = null;
			PreparedStatement ps = null;
			try {
				conn = dataSource.getConnection();
				ps = conn.prepareStatement("UPDATE UCDWIRELESSM1 SET PRIMARY_USE=?, ACCESS_POINTS=?, WIRELESS_COVERAGE=?, COVERAGE_NOTES=?, SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER=? WHERE LOCATION_CODE=?");
				ps.setString(1, req.getParameter("usage"));
				ps.setString(2, req.getParameter("accessPoints"));
				ps.setString(3, req.getParameter("coverage"));
				ps.setString(4, req.getParameter("details"));
				ps.setString(5, authenticatedUser);
				ps.setString(6, req.getParameter("id"));
				int rowCt = ps.executeUpdate();
				if (rowCt > 0) {
					if (log.isDebugEnabled()) {
						log.debug("Building information successfully updated.");
					}
					string = "true";
				} else {
					try {
						ps.close();
					} catch (Exception e1) {
						// no one cares!
					}
					ps = conn.prepareStatement("INSERT INTO UCDWIRELESSM1 (ID, LOCATION_CODE, PRIMARY_USE, ACCESS_POINTS, WIRELESS_COVERAGE, COVERAGE_NOTES, SYSMODTIME, SYSMODCOUNT, SYSMODUSER) VALUES (?, ?, ?, ?, ?, ?, GETDATE(), 1, ?)");
					ps.setString(1, req.getParameter("id"));
					ps.setString(2, req.getParameter("id"));
					ps.setString(3, req.getParameter("usage"));
					ps.setString(4, req.getParameter("accessPoints"));
					ps.setString(5, req.getParameter("coverage"));
					ps.setString(6, req.getParameter("details"));
					ps.setString(7, authenticatedUser);
					rowCt = ps.executeUpdate();
					if (rowCt > 0) {
						if (log.isDebugEnabled()) {
							log.debug("Building information successfully inserted.");
						}
						string = "true";
					}
				}
			} catch (SQLException e) {
				log.error("Exception encountered updating building information: " + e.getMessage(), e);
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
		}

		return string;
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param id the id of the requested building
	 * @return the Javascript response
	 */
	@SuppressWarnings("unchecked")
	private String processSingleRequest(String id) {
		JSONObject item = new JSONObject();

		if (log.isDebugEnabled()) {
			log.debug("Fetching building information for id " + id);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement("SELECT a.LOCATION, a.LOCATION_NAME, a.ADDRESS, a.CITY, a.STATE, a.ZIP, a.LOCATION_CODE, a.LONGITUDE, a.LATITUDE, b.PRIMARY_USE, b.ACCESS_POINTS, b.WIRELESS_COVERAGE, b.COVERAGE_NOTES, a.LOCATION_FULL_NAME FROM LOCM1 AS a LEFT OUTER JOIN UCDWIRELESSM1 AS b ON b.LOCATION_CODE = a.LOCATION_CODE WHERE a.LOCATION_CODE=?");
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				item.put("id", rs.getString("LOCATION"));
				item.put("name", rs.getString("LOCATION_NAME"));
				item.put("fullName", rs.getString("LOCATION_FULL_NAME"));
				item.put("address", rs.getString("ADDRESS"));
				item.put("city", rs.getString("CITY"));
				item.put("state", rs.getString("STATE"));
				item.put("zip", rs.getString("ZIP"));
				item.put("code", rs.getString("LOCATION_CODE"));
				item.put("longitude", rs.getString("LONGITUDE"));
				item.put("latitude", rs.getString("LATITUDE"));
				item.put("usage", rs.getString("PRIMARY_USE"));
				item.put("accessPoints", rs.getInt("ACCESS_POINTS") + "");
				item.put("wirelessCoverage", rs.getString("WIRELESS_COVERAGE"));
				item.put("details", rs.getString("COVERAGE_NOTES"));
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing building information: " + e.getMessage(), e);
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

		return item.toJSONString();
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @return the Javascript response
	 */
	@SuppressWarnings("unchecked")
	private String processFullListRequest() {
		JSONArray list = new JSONArray();

		if (log.isDebugEnabled()) {
			log.debug("Fetching building information ...");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement("SELECT a.LOCATION, a.LOCATION_NAME, a.ADDRESS, a.CITY, a.STATE, a.ZIP, a.LOCATION_CODE, a.LONGITUDE, a.LATITUDE, b.PRIMARY_USE, b.ACCESS_POINTS, b.WIRELESS_COVERAGE, b.COVERAGE_NOTES, a.LOCATION_FULL_NAME FROM LOCM1 AS a LEFT OUTER JOIN UCDWIRELESSM1 AS b ON b.LOCATION_CODE = a.LOCATION_CODE WHERE [LEVEL]=1 ORDER BY a.LOCATION_FULL_NAME");
			rs = ps.executeQuery();
			while (rs.next()) {
				JSONObject item = new JSONObject();
				item.put("id", rs.getString("LOCATION"));
				item.put("name", rs.getString("LOCATION_NAME"));
				item.put("fullName", rs.getString("LOCATION_FULL_NAME"));
				item.put("address", rs.getString("ADDRESS"));
				item.put("city", rs.getString("CITY"));
				item.put("state", rs.getString("STATE"));
				item.put("zip", rs.getString("ZIP"));
				item.put("code", rs.getString("LOCATION_CODE"));
				item.put("longitude", rs.getString("LONGITUDE"));
				item.put("latitude", rs.getString("LATITUDE"));
				item.put("usage", rs.getString("PRIMARY_USE"));
				item.put("accessPoints", rs.getInt("ACCESS_POINTS") + "");
				item.put("wirelessCoverage", rs.getString("WIRELESS_COVERAGE"));
				item.put("details", rs.getString("COVERAGE_NOTES"));
				list.add(item);
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing building information: " + e.getMessage(), e);
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

		return list.toJSONString();
	}
}
