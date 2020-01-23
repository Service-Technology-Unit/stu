package edu.ucdavis.ucdh.stu.stu.servlets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
 * <p>This servlet verifies the currently authenticated user's authorization.</p>
 */
public class AuthorizationServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	DataSource dataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/checkauthority.js";
		defaultVar = "authority";
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

		if (StringUtils.isNotEmpty(req.getParameter("pathlist"))) {
			response = processListRequest(req);
		} else {
			response = isAuthorized(req, req.getHeader("Referer"));
		}

		return response;
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the Javascript response
	 */
	@SuppressWarnings("unchecked")
	private String processListRequest(HttpServletRequest req) {
		JSONArray list = new JSONArray();

		String pathlist = req.getParameter("pathlist");
		if (log.isDebugEnabled()) {
			log.debug("Determining authorization for the following path list: " + pathlist);
		}
		String[] path = pathlist.split(",");
		for (int i=0; i<path.length; i++) {
			JSONObject item = new JSONObject();
			item.put("path", path[i]);
			item.put("authorized", isAuthorized(req, path[i]));
			list.add(item);
		}

		return list.toJSONString();
	}

	/**
	 * <p>Returns a String value of true or false based on the user's
	 * authorization for the path specified.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param path the path for which we are checking autorization
	 * @return a String value of true or false based on the user's
	 * authorization for the path specified
	 */
	private String isAuthorized(HttpServletRequest req, String path) {
		String authorized = "true";

		if (log.isDebugEnabled()) {
			log.debug("Determining authorization for the following path: " + path);
		}
		if (StringUtils.isEmpty(path)) {
			authorized = "false";
		} else {
			if (path.startsWith("http")) {
				String[] parts = path.split("/");
				String server = parts[0] + "//" + parts[2];
				path = path.substring(server.length());
			}
			if (path.indexOf("?") != -1) {
				path = path.substring(0, path.indexOf("?"));
			}
			if (log.isDebugEnabled()) {
				log.debug("Adjusted path is " + path);
			}
			Contact user = (Contact) req.getSession().getAttribute(USER_DETAILS);
			if (user == null) {
				if (StringUtils.isNotEmpty((String) req.getSession().getAttribute(AUTHENTICATED_USER))) {
					if (log.isDebugEnabled()) {
						log.debug("User is null; authenticatedUser is " + req.getSession().getAttribute(AUTHENTICATED_USER));
					}
					authorized = "false";
				} else {
					if (log.isDebugEnabled()) {
						log.debug("There is no authenticated user associated with this session");
					}
					authorized = "null";
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("User: " + user.getId() + " (" + user.getUcdLoginId() + ")");
				}
				Map<String,String> auth = fetchAuthorization(path);
				if (auth != null) {
					String type = auth.get("type");
					if (log.isDebugEnabled()) {
						log.debug("Authorization type: " + type);
					}
					if (StringUtils.isEmpty(auth.get("authorized")) && !"it".equalsIgnoreCase(type)) {
						authorized = "false";
					} else {
						List<String> list = stringToList(auth.get("authorized"));
						if ("user".equalsIgnoreCase(type)) {
							authorized = checkUserAuthorization(req, list, user);
						} else if ("department".equalsIgnoreCase(type)) {
							authorized = checkDeptAuthorization(req, list, user);
						} else if ("group".equalsIgnoreCase(type)) {
							authorized = checkGroupAuthorization(req, list, user);
						} else if ("team".equalsIgnoreCase(type)) {
							authorized = checkTeamAuthorization(req, list);
						} else if ("it".equalsIgnoreCase(type)) {
							authorized = checkItAuthorization(req, user);
						} else {
							log.warn("Invalid authorization type: " + type);
							authorized = "false";
						}
					}
				}
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("Authorized = " + authorized);
		}

		return authorized;
	}

	/**
	 * <p>Returns a map of the database fields related to the specified path.</p>
	 *
	 * @return a map of the database fields related to the specified path
	 */
	private Map<String,String> fetchAuthorization(String path) {
		Map<String,String> map = null;

		if (log.isDebugEnabled()) {
			log.debug("Reading authorization data for path " + path);
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement("SELECT AUTHORIZATION_TYPE, AUTHORIZATION_LIST FROM UCDWEBSECM1 WHERE PATH=?");
			ps.setString(1, path);
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Authorization data found for path " + path);
				}
				map = new HashMap<String,String>();
				map.put("type", rs.getString("AUTHORIZATION_TYPE"));
				map.put("authorized", rs.getString("AUTHORIZATION_LIST"));
			}
		} catch (SQLException e) {
			log.error("Exception encountered while attempting to access path " + path + ": " + e.getMessage(), e);
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

		return map;
	}

	/**
	 * <p>Converts the string to a list.</p>
	 *
	 * @param string the string
	 * @return the list
	 */
	private List<String> stringToList(String string) {
		List<String> list = new ArrayList<String>();

		if (StringUtils.isNotEmpty(string)) {
			String[] stringArray = string.split("\n");
			for (int i=0; i<stringArray.length; i++) {
				list.add(stringArray[i].trim());
			}
		}

		return list;
	}

	/**
	 * <p>Returns true if the current user is on the list.</p>
	 *
	 * @return true if the current user is on the list
	 */
	private String checkUserAuthorization(HttpServletRequest req, List<String> list, Contact user) {
		return list.contains(user.getUcdLoginId()) + "";
	}

	/**
	 * <p>Returns true if the current user's department is on the list.</p>
	 *
	 * @return true if the current user's department is on the list
	 */
	private String checkDeptAuthorization(HttpServletRequest req, List<String> list, Contact user) {
		String authorized = "false";

		String department = user.getDepartment();
		if (StringUtils.isNotEmpty(department)) {
			authorized = list.contains(department) + "";
		}

		return authorized;
	}

	/**
	 * <p>Returns true if any assignment group is on the list.</p>
	 *
	 * @return true if any assignment group is on the list
	 */
	private String checkGroupAuthorization(HttpServletRequest req, List<String> list, Contact user) {
		String authorized = "false";

		if (user.getGroup() != null && user.getGroup().size() > 0) {
			Iterator<String> i = list.iterator();
			while (i.hasNext()) {
				if (user.getGroup().contains(i.next())) {
					authorized = "true";
				}
			}
		}

		return authorized;
	}

	/**
	 * <p>Returns true if any team is on the list.</p>
	 *
	 * @return true if any team is on the list
	 */
	private String checkTeamAuthorization(HttpServletRequest req, List<String> list) {
		String authorized = "false";


		return authorized;
	}

	/**
	 * <p>Returns true if the current user's department is an IT department.</p>
	 *
	 * @return true if the current user's department is an IT department
	 */
	private String checkItAuthorization(HttpServletRequest req, Contact user) {
		String authorized = "false";

		if ("t".equalsIgnoreCase(user.getItStaff())) {
			authorized = "true";
		}

		return authorized;
	}
}
