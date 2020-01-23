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
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.stu.beans.Contact;

/**
 * <p>This is the universal JSON servlet.</p>
 */
public class UniversalJsonServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private static final String SQL = "select id, updatesDatabase, authenticationRequired, authorizationList, datasource, sql from JSONQuery where id=?";
	DataSource dataSource = null;
	Map<String,DataSource> dataSourceMap = new HashMap<String,DataSource>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/json.js";
		defaultVar = "result";
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("utilDataSource");
	}

	/**
	 * <p>Returns the Javascript results.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript array of the search results
	 * @throws IOException 
	 */
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";

		String qid = req.getParameter("qid");
		if (StringUtils.isNotEmpty(qid)) {
			Map<String,String> query = fetchQuery(qid);
			if (query != null) {
				response = processQuery(req, res, query);
			} else {
				sendError(req, res, HttpServletResponse.SC_BAD_REQUEST, "No query on file with an id of \"" + qid + "\".");
			}
		} else {
			sendError(req, res, HttpServletResponse.SC_BAD_REQUEST, "Required parameter \"qid\" missing or invalid.");
		}

		return response;
	}

	/**
	 * <p>Returns the requested query details.</p>
	 *
	 * @param qid the id of the requested query
	 * @return the requested query details
	 */
	private Map<String,String> fetchQuery(String qid) {
		Map<String,String> query = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching JSON query " + qid);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + SQL);
			}
			ps = conn.prepareStatement(SQL);
			ps.setString(1, qid);
			rs = ps.executeQuery();
			if (rs.next()) {
				query = new HashMap<String,String>();
				query.put("updatesDatabase", rs.getString("updatesDatabase"));
				query.put("authenticationRequired", rs.getString("authenticationRequired"));
				query.put("authorizationList", rs.getString("authorizationList"));
				query.put("datasource", rs.getString("datasource"));
				query.put("sql", rs.getString("sql"));
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing JSON query data: " + e.getMessage(), e);
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

		return query;
	}

	/**
	 * <p>Returns the Javascript results.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @param query the requested query to execute
	 * @return the Javascript array of the search results
	 * @throws IOException 
	 */
	private String processQuery(HttpServletRequest req, HttpServletResponse res, Map<String,String> query) throws IOException {
		String response = "";

		boolean authorized = true;
		if (!"0".equals(query.get("authenticationRequired"))) {
			String authenticatedUser = (String) req.getSession().getAttribute(AUTHENTICATED_USER);
			if (StringUtils.isEmpty(authenticatedUser)) {
				authorized = false;
				sendError(req, res, HttpServletResponse.SC_FORBIDDEN, "Authentication required.");
			} else {
				if (StringUtils.isNotEmpty(query.get("authorizationList"))) {
					if (query.get("authorizationList").indexOf(authenticatedUser) == -1) {
						authorized = false;
						sendError(req, res, HttpServletResponse.SC_FORBIDDEN, "You are not authorized for this service.");
					}
				}
			}
		}
		if (authorized) {
			DataSource ds = fetchDataSource(query);
			if (ds != null) {
				if (!"0".equals(query.get("updatesDatabase"))) {
					response = processUpdateRequest(req, query);
				} else {
					response = processFetchRequest(req, query);
				}
			} else {
				sendError(req, res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "The specified datasource is not available.");
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
	private String processUpdateRequest(HttpServletRequest req, Map<String,String> query) {
		String string = "false";

		if (log.isDebugEnabled()) {
			log.debug("Updating database ...");
		}
		DataSource ds = fetchDataSource(query);
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			String sql = resolveVariables(req, query.get("sql"));
			conn = ds.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			int parameterCt = StringUtils.countMatches(sql, "?");
			for (int i=0; i<parameterCt; i++) {
				int p = i + 1;
				ps.setString(p, fixValue(req.getParameter("p" + p)));
			}
			int rowCt = ps.executeUpdate();
			if (rowCt > 0) {
				if (log.isDebugEnabled()) {
					log.debug(rowCt + " rows affected by this operation");
				}
				string = "true";
			} else {
				log.error("No rows were affected by the update operation");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to update database: " + e.getMessage(), e);
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

		return string;
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the Javascript response
	 */
	@SuppressWarnings("unchecked")
	private String processFetchRequest(HttpServletRequest req, Map<String,String> query) {
		JSONArray list = new JSONArray();

		if (log.isDebugEnabled()) {
			log.debug("Fetching data from the database...");
		}
		DataSource ds = fetchDataSource(query);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = resolveVariables(req, query.get("sql"));
			conn = ds.getConnection();
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + sql);
			}
			ps = conn.prepareStatement(sql);
			int parameterCt = StringUtils.countMatches(sql, "?");
			for (int i=0; i<parameterCt; i++) {
				int p = i + 1;
				ps.setString(p, fixValue(req.getParameter("p" + p)));
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				JSONObject item = new JSONObject();
				int colCt = rs.getMetaData().getColumnCount();
				for (int i=0; i<colCt; i++) {
					String colName = rs.getMetaData().getColumnName(i+1);
					String jsonName = fixName(colName);
					item.put(jsonName, rs.getString(colName));
				}
				list.add(item);
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing retrieved data: " + e.getMessage(), e);
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

	private String fixValue(String value) {
		if (StringUtils.isNotEmpty(value)) {
			value = value.trim();
		} else {
			value = "";
		}
		if (value.equalsIgnoreCase("null")) {
			value = null;
		}
		return value;
	}

	private String resolveVariables(HttpServletRequest req, String sql) {
		String resolved = "";

		if (StringUtils.isNotEmpty(sql)) {
			String remoteAddr = req.getRemoteAddr();
			if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
				remoteAddr = req.getHeader("X-Forwarded-For");
			}
			resolved = sql.replace("$remoteAddr", "'" + remoteAddr + "'");
			resolved = resolved.replace("$remoteHost", "'" + req.getRemoteHost() + "'");
			resolved = resolved.replace("$userAgent", "'" + req.getHeader("User-Agent") + "'");
			if (resolved.indexOf("$remoteUser") != -1) {
				Contact userDetails = (Contact) req.getSession().getAttribute(USER_DETAILS);
				if (userDetails != null) {
					resolved = resolved.replace("$remoteUser", "'" + userDetails.getId() + "'");
				} else {
					resolved = resolved.replace("$remoteUser", "null");
				}
			}
		}
		return resolved;
	}

	private String fixName(String name) {
		if (name.indexOf("_") != -1) {
			name = name.toLowerCase();
			name = name.replace("_a", "A");
			name = name.replace("_b", "B");
			name = name.replace("_c", "C");
			name = name.replace("_d", "D");
			name = name.replace("_e", "E");
			name = name.replace("_f", "F");
			name = name.replace("_g", "G");
			name = name.replace("_h", "H");
			name = name.replace("_i", "I");
			name = name.replace("_j", "J");
			name = name.replace("_k", "K");
			name = name.replace("_l", "L");
			name = name.replace("_m", "M");
			name = name.replace("_n", "N");
			name = name.replace("_o", "O");
			name = name.replace("_p", "P");
			name = name.replace("_q", "Q");
			name = name.replace("_r", "R");
			name = name.replace("_s", "S");
			name = name.replace("_t", "T");
			name = name.replace("_u", "U");
			name = name.replace("_v", "V");
			name = name.replace("_w", "W");
			name = name.replace("_x", "X");
			name = name.replace("_y", "Y");
			name = name.replace("_z", "Z");
			name = name.replace("_", "");
		} else {
			if (name.equals(name.toUpperCase())) {
				name = name.toLowerCase();
			}
		}
		return name;
	}

	/**
	 * <p>Returns the requested datasource.</p>
	 *
	 * @param query the query details
	 * @return the requested datasource
	 */
	private DataSource fetchDataSource(Map<String,String> query) {
		DataSource ds = null;

		String dsName = query.get("datasource");
		if (StringUtils.isNotEmpty(dsName)) {
			if (dataSourceMap.containsKey(dsName)) {
				ds = dataSourceMap.get(dsName);
			} else {
				if ("utilDataSource".equals(dsName)) {
					ds = dataSource;
					dataSourceMap.put(dsName, ds);
				} else {
					if (dsName.startsWith("jdbc/")) {
						Hashtable<String,String> parms = new Hashtable<String,String>();
						try {
							Context ctx = new InitialContext(parms);
							ds = (DataSource) ctx.lookup("java:comp/env/" + dsName);
							dataSourceMap.put(dsName, ds);
						} catch (Exception e) {
							log.error("Exception encountered attempting to obtain datasource " + dsName, e);
						}
					} else {
						ServletConfig config = getServletConfig();
						try {
							ds = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean(dsName);
							dataSourceMap.put(dsName, ds);
						} catch (Exception e) {
							log.error("Exception encountered attempting to obtain datasource " + dsName, e);
						}
					}
				}
			}
		} else {
			log.error("DataSource name not specified.");
		}

		return ds;
	}
}
