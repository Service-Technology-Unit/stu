package edu.ucdavis.ucdh.stu.stu.servlets;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONObject;

/**
 * <p>This servlet verifies database connection parameters.</p>
 */
public class ConnectionTestServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private List<String> loadedDrivers = new ArrayList<String>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/conntest.js";
	}

	/**
	 * <p>Returns the Javascript object for the list of locations.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript object for the list of locations
	 */
	@SuppressWarnings("unchecked")
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		JSONObject jsonResponse = new JSONObject();

		if (log.isDebugEnabled()) {
			log.debug("Verifying connection parameter values ...");
		}
		String driverClass = req.getParameter("drv");
		String connectionString = req.getParameter("url");
		String userId = req.getParameter("uid");
		String password = req.getParameter("pwd");
		String sql = req.getParameter("sql");
		if (StringUtils.isNotEmpty(driverClass)) {
			if (StringUtils.isNotEmpty(connectionString)) {
				if (log.isDebugEnabled()) {
					log.debug("Testing connection for " + connectionString + " using driver " + driverClass);
				}
				if (driverLoaded(driverClass)) {
					if (log.isDebugEnabled()) {
						log.debug("Driver " + driverClass + " loaded successfully");
					}
					try {
						Connection conn = DriverManager.getConnection(connectionString, userId, password);
						if (log.isDebugEnabled()) {
							log.debug("Connection established");
						}
						DatabaseMetaData dbMetaData = conn.getMetaData();
						jsonResponse.put("driverName", dbMetaData.getDriverName());
						jsonResponse.put("driverVersion", dbMetaData.getDriverVersion());
						jsonResponse.put("databaseProductName", dbMetaData.getDatabaseProductName());
						jsonResponse.put("databaseProductVersion", dbMetaData.getDatabaseProductVersion());
						if (StringUtils.isNotEmpty(sql)) {
							if (log.isDebugEnabled()) {
								log.debug("Testing connection with the following SQL statement: " + sql);
							}
							try {
								Statement stmt = conn.createStatement();
								ResultSet rs = stmt.executeQuery(sql);
								if (rs.next()) {
									jsonResponse.put("result", rs.getString(1));
									jsonResponse.put("response", "success");
									jsonResponse.put("message", "Connection established; SQL successful");
								} else {
									jsonResponse = failure(jsonResponse, "SQL returned no results");
								}
								try {
									rs.close();
									stmt.close();
								} catch (SQLException e) {
									// no one cares
								}
							} catch (SQLException e) {
								jsonResponse = failure(jsonResponse, "Exception encountered attempting to execute SQL: " + e.toString(), e);
								jsonResponse.put("exception", e.toString());
								jsonResponse.put("exceptionMessage", e.getMessage());
								jsonResponse.put("stackTrace", getStackTrace(e));
							}
						} else {
							jsonResponse.put("response", "success");
							jsonResponse.put("message", "Connection established; no SQL attempted");
						}
						try {
							conn.close();
						} catch (SQLException e) {
							// no one cares
						}
					} catch (SQLException e) {
						jsonResponse = failure(jsonResponse, "Exception encountered attempting to connect to database: " + e.toString(), e);
						jsonResponse.put("exception", e.toString());
						jsonResponse.put("exceptionMessage", e.getMessage());
						jsonResponse.put("stackTrace", getStackTrace(e));
					}
				} else {
					jsonResponse = failure(jsonResponse, "Unable to load driver class " + driverClass);
				}
			} else {
				jsonResponse = failure(jsonResponse, "Required parameter connection string (url) not provided");
			}
		} else {
			jsonResponse = failure(jsonResponse, "Required parameter driver class (drv) not provided");
		}

		return jsonResponse.toJSONString();
	}

	/**
	 * <p>Loads the JDBC driver.</p>
	 *
	 * @param driverClass the class name of the JDBC driver
	 * @return true if the driver class can be loaded
	 */
	private boolean driverLoaded(String driverClass) {
		boolean loaded = false;

		if (loadedDrivers.contains(driverClass)) {
			loaded = true;
		} else {
			try {
				if (log.isDebugEnabled()) {
					log.debug("Attempting to load driver " + driverClass);
				}
				Class.forName(driverClass);
				loadedDrivers.add(driverClass);
				loaded = true;
			} catch (ClassNotFoundException e) {
				if (log.isDebugEnabled()) {
					log.debug("Exception encountered when attempting to load driver " + driverClass + ": " + e.getMessage(), e);
				}
			}
		}

		return loaded;
	}

	/**
	 * <p>Formats the stack trace found in the exception passed.</p>
	 *
	 * @param t the Throwable object
	 * @return the formatted stack trace
	 */
	private String getStackTrace(Throwable t) {
		String stackTrace = "";

		StackTraceElement[] line = t.getStackTrace();
		String separator = "";
		for (int i=0; i<line.length; i++) {
			stackTrace += separator;
			stackTrace += line[i].toString();
			separator = " at\n";
		}

		return stackTrace;
	}

	/**
	 * <p>Processes the failed connection test.</p>
	 *
	 * @param jsonResponse the jsonResponse object
	 * @param message the explanation of the failure
	 * @return the updated jsonResponse object
	 */
	private JSONObject failure(JSONObject jsonResponse, String message) {
		return failure(jsonResponse, message, null);
	}

	/**
	 * <p>Processes the failed connection test.</p>
	 *
	 * @param jsonResponse the jsonResponse object
	 * @param message the explanation of the failure
	 * @param t the Throwable object
	 * @return the updated jsonResponse object
	 */
	@SuppressWarnings("unchecked")
	private JSONObject failure(JSONObject jsonResponse, String message, Throwable t) {
		jsonResponse.put("response", "fail");
		jsonResponse.put("error", message);
		if (log.isDebugEnabled()) {
			if (t != null) {
				log.debug("Connection test failed: " + message, t);
			} else {
				log.debug("Connection test failed: " + message);
			}
		}
		return jsonResponse;
	}
}
