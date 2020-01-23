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
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * <p>This servlet produces a csv file of the data returned by a stored query statement.</p>
 */
public class UniversalCsvServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private static final String AUTHENTICATED_USER = "authenticatedUser";
	private Log log = LogFactory.getLog(getClass());
	private static final String SQL = "select id, updatesDatabase, authenticationRequired, authorizationList, datasource, sql from JSONQuery where id=?";
	DataSource dataSource = null;
	Map<String,DataSource> dataSourceMap = new HashMap<String,DataSource>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		if (log.isDebugEnabled()) {
			log.debug("UniversalCsvServlet initializing ...");
		}
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("utilDataSource");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = processRequest(req, res);
		res.setCharacterEncoding("UTF-8");
		res.setContentType("text/comma-separated-values;charset=UTF-8");
		res.getWriter().write(response);
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
	 * <p>Returns the csv text for the requested data.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the csv text for the requested data
	 * @throws IOException 
	 */
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) throws IOException {
		String response = "";

		String qid = getQid(req);
		if (StringUtils.isNotEmpty(qid)) {
			Map<String,String> query = fetchQuery(qid);
			if (query != null) {
				if ("0".equals(query.get("updatesDatabase"))) {
					response = processQuery(req, res, query);
				} else {
					sendError(req, res, HttpServletResponse.SC_BAD_REQUEST, "No valid query on file with an id of \"" + qid + "\".");
				}
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
				response = processFetchRequest(req, query);
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
	private String processFetchRequest(HttpServletRequest req, Map<String,String> query) {
		StringBuffer response = new StringBuffer();

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
				ps.setString(p, req.getParameter("p" + p));
			}
			boolean firstTime = true;
			rs = ps.executeQuery();
			while (rs.next()) {
				int colCt = rs.getMetaData().getColumnCount();
				String separator = "\n";
				if (firstTime) {
					separator = "";
					for (int i=0; i<colCt; i++) {
						response.append(separator);
						response.append("\"");
						response.append(fixName(rs.getMetaData().getColumnName(i+1)));
						response.append("\"");
						separator = ",";
					}
					firstTime = false;
					separator = "\n";
				}
				for (int i=0; i<colCt; i++) {
					response.append(separator);
					response.append("\"");
					response.append(fixValue(rs.getString(i+1)));
					response.append("\"");
					separator = ",";
				}
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

		return response.toString();
	}

	private String resolveVariables(HttpServletRequest req, String sql) {
		String resolved = "";

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		if (StringUtils.isNotEmpty(sql)) {
			resolved = sql.replace("$remoteAddr", "'" + remoteAddr + "'");
			resolved = resolved.replace("$remoteHost", "'" + remoteAddr + "'");
			if (resolved.indexOf("$remoteUser") != -1) {
				if (StringUtils.isNotEmpty(req.getRemoteUser())) {
					resolved = resolved.replace("$remoteUser", "'" + req.getRemoteUser() + "'");
				} else {
					resolved = resolved.replace("$remoteUser", "null");
				}
			}
		}
		return resolved;
	}

	private String fixValue(String value) {
		if (StringUtils.isNotEmpty(value) && !"null".equalsIgnoreCase(value)) {
			value = value.replace("\r\n", "\n");
			value = value.replace("\n\r", "\n");
			value = value.replace("\r", "\n");
			value = value.replace("\n", "\\n");
			value = value.replace("\"", "\\\"");
		} else {
			value = "";
		}
		return value;
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

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the id present in the URL, if any
	 */
	private String getQid(HttpServletRequest req) {
		String qid = null;

		String basePath = req.getContextPath() + "/";
		if (req.getRequestURI().length() > basePath.length()) {
			qid = req.getRequestURI().substring(basePath.length());
			if (qid.indexOf(".csv") != -1) {
				qid = qid.substring(0, qid.indexOf(".csv"));
			}
		}

		return qid;
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
