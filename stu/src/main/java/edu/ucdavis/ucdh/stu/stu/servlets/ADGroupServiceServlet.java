package edu.ucdavis.ucdh.stu.stu.servlets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * <p>This servlet adds and removes users from Active Directory groups.</p>
 */
public class ADGroupServiceServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;
	private static final String INITIAL_CONTEXT_FACTORY = "com.sun.jndi.ldap.LdapCtxFactory";
	private static final String SECURITY_AUTHENTICATION = "simple";
	private static final String AUDIT_SQL = "INSERT INTO AD_GROUPAUDIT (ACTION, USER_ID, GROUP_ID, RESPONSE_CODE, RESPONSE, SYSMODTIME, SYSMODUSER, SYSMODCOUNT, SYSMODADDR) VALUES(?, ?, ?, ?, ?, GETDATE(), ?, 0, ?)";
	private Log log = LogFactory.getLog(getClass());
	private DataSource dataSource = null;
	private String providerURL = null;
	private String securityPrincipal = null;
	private String securityCredentials = null;
	private String authorizedIpList = null;
	private String baseUserFilterList = null;
	private String baseGroupFilterList = null;
	private List<String> authorizedIp = new ArrayList<String>();
	private List<String> baseUserFilter = new ArrayList<String>();
	private List<String> baseGroupFilter = new ArrayList<String>();

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/adgroup.js";
		ServletContext context = getServletConfig().getServletContext();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("utilDataSource");
		providerURL = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServiceURL");
		securityPrincipal = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServiceUser");
		securityCredentials = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServicePassword");
		authorizedIpList = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServiceIpList");
		if (StringUtils.isNotEmpty(authorizedIpList)) {
			String[] ipArray = authorizedIpList.split(",");
			for (int i=0; i<ipArray.length; i++) {
				authorizedIp.add(ipArray[i].trim());
			}
		}
		baseUserFilterList = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServiceUserFilterList");
		if (StringUtils.isNotEmpty(baseUserFilterList)) {
			String[] filterArray = baseUserFilterList.split("\n");
			for (int i=0; i<filterArray.length; i++) {
				baseUserFilter.add(filterArray[i].trim());
			}
		}
		baseGroupFilterList = (String) WebApplicationContextUtils.getRequiredWebApplicationContext(context).getBean("groupServiceGroupFilterList");
		if (StringUtils.isNotEmpty(baseGroupFilterList)) {
			String[] filterArray = baseGroupFilterList.split("\n");
			for (int i=0; i<filterArray.length; i++) {
				baseGroupFilter.add(filterArray[i].trim());
			}
		}
		if (log.isDebugEnabled()) {
			log.debug("providerURL: " + providerURL);
			log.debug("securityPrincipal: " + securityPrincipal);
			log.debug("securityCredentials: ********");
			log.debug("authorizedIpList: " + authorizedIpList);
			log.debug("baseUserFilterList: " + baseUserFilterList);
			log.debug("baseGroupFilterList: " + baseGroupFilterList);
		}
	}

	/**
	 * <p>Returns the Javascript response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 */
	@SuppressWarnings("unchecked")
	protected String processRequest(HttpServletRequest req, HttpServletResponse res) {
		JSONObject response = new JSONObject();

		String user = req.getParameter("u");
		String group = req.getParameter("g");
		String action = req.getParameter("a");
		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		if (!authorizedIp.contains(remoteAddr)) {
			response.put("responseCode", 403);
			response.put("response", "IP Address " + remoteAddr + " is not authorized for this service.");
		} else if (StringUtils.isEmpty(user)) {
			response.put("responseCode", 1);
			response.put("response", "Required parameter \"u\" (user) missing or invalid.");
		} else if (StringUtils.isEmpty(group)) {
			response.put("responseCode", 2);
			response.put("response", "Required parameter \"g\" (group) missing or invalid.");
		} else if (StringUtils.isNotEmpty(action) && !"add".equalsIgnoreCase(action) && !"remove".equalsIgnoreCase(action)) {
			response.put("responseCode", 3);
			response.put("response", "Parameter \"a\" (action) contains an invalid value: " + action);
		} else {
			if (StringUtils.isEmpty(action)) {
				action = "add";
			}
			try {
				Hashtable<String,String> env = new Hashtable<String,String>();
				env.put(Context.INITIAL_CONTEXT_FACTORY, INITIAL_CONTEXT_FACTORY);
				env.put(Context.PROVIDER_URL, providerURL);					   
				env.put(Context.SECURITY_AUTHENTICATION, SECURITY_AUTHENTICATION);					   
				env.put(Context.SECURITY_PRINCIPAL, securityPrincipal);	
				env.put(Context.SECURITY_CREDENTIALS, securityCredentials);
				DirContext ctx = new InitialDirContext(env);
				Object result = getUserDistinguishedName(ctx, user);
				String userDN = result.toString();
				if (result.equals(userDN)) {
					if (userDN.startsWith("Multiple")) {
						response.put("responseCode", 4);
						response.put("response", userDN);
					} else if ("User Not Found".equalsIgnoreCase(userDN)) {
						response.put("responseCode", 5);
						response.put("response", "User \"" + user + "\" was not found in the directory.");
					} else if ("User Disabled".equalsIgnoreCase(userDN)) {
						response.put("responseCode", 6);
						response.put("response", "The account for user \"" + user + "\" is disabled.");
					} else {
						response.put("responseCode", 99);
						response.put("response", userDN);
					}
				} else {
					SearchResult userObject = (SearchResult) result;
					userDN = userObject.getNameInNamespace();
					String groupDN = getGroupDistinguishedName(ctx, group, userObject, action);
					if (groupDN.startsWith("Exception")) {
						response.put("responseCode", 99);
						response.put("response", groupDN);
					} else if (groupDN.startsWith("Multiple")) {
						response.put("responseCode", 7);
						response.put("response", groupDN);
					} else if ("Group Not Found".equalsIgnoreCase(groupDN)) {
						response.put("responseCode", 8);
						response.put("response", "Group \"" + group + "\" was not found in the directory.");
					} else if ("User already a member of this group".equalsIgnoreCase(groupDN)) {
						response.put("responseCode", 9);
						response.put("response", "User \"" + user + "\" is already a member of group \"" + group + "\".");
					} else if ("User not a member of this group".equalsIgnoreCase(groupDN)) {
						response.put("responseCode", 10);
						response.put("response", "User \"" + user + "\" is not a member of group \"" + group + "\".");
					} else {
						BasicAttribute member = new BasicAttribute("member", userDN);
						Attributes atts = new BasicAttributes();
						atts.put(member);
						int command = LdapContext.ADD_ATTRIBUTE;
						if ("remove".equalsIgnoreCase(action)) {
							command = LdapContext.REMOVE_ATTRIBUTE;
						}
						try {
							ctx.modifyAttributes(groupDN, command, atts);
						} catch (NameAlreadyBoundException e) {
							response.put("responseCode", 9);
							response.put("response", "User \"" + user + "\" is already a member of group \"" + group + "\".");
						}
						response.put("responseCode", 0);
						response.put("response", "User \"" + user + "\" added to group \"" + group + "\".");
						if ("remove".equalsIgnoreCase(action)) {
							response.put("response", "User \"" + user + "\" removed from group \"" + group + "\".");
						}
					}
				}
				ctx.close();
			} catch (Exception e) {
				response.put("responseCode", 99);
				response.put("response", "Exception encountered adding user \"" + user + "\" to group \"" + group + "\": " + e);
				log.error("Exception encountered adding user \"" + user + "\" to group \"" + group + "\": " + e, e);
			}
		}
		logActivity(req, response, action, user, group, remoteAddr);

		return response.toJSONString();
	}

	private Object getUserDistinguishedName(DirContext ctx, String user) {
		Object response = "Not Found";

		if (log.isDebugEnabled()) {
			log.debug("Searching for user " + user);
		}
		SearchControls ctls = new SearchControls();		
		ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		try {
			for (int i=0; i<baseUserFilter.size(); i++) {
				NamingEnumeration<SearchResult> answer = ctx.search(baseUserFilter.get(i), "sAMAccountName=" + user, ctls);
				int count = 0;
				int disabled = 0;
				while (answer != null && answer.hasMore()) {
					SearchResult sr = answer.next();
					response = sr;
					disabled = Integer.parseInt(sr.getAttributes().get("userAccountControl").get().toString()) % 16;
					count++;
				}
				if (count == 0) {
					response = "User Not Found";
				} else { 
					i = baseUserFilter.size();
					if (count > 1) {
						response = "Multiple user records found (" + count + ")";
					} else if (disabled == 2) {
						response = "User Disabled";
					}
				}
			}
		} catch (Exception e) {
			response = "Exception encountered searching for user " + user + ": " + e;
			log.error("Exception encountered searching for user " + user + ": " + e, e);
		}
		if (log.isDebugEnabled()) {
			log.debug("Response: " + response);
		}

		return response;
	}

	private String getGroupDistinguishedName(DirContext ctx, String group, SearchResult userObject, String action) {
		String response = "Not Found";

		if (log.isDebugEnabled()) {
			log.debug("Searching for group " + group);
		}
		String userDN = userObject.getNameInNamespace();
		String groupDN = "";
		SearchControls ctls = new SearchControls();		
		ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);
		try {
			for (int i=0; i<baseGroupFilter.size(); i++) {
				NamingEnumeration<SearchResult> answer = ctx.search(baseGroupFilter.get(i), "sAMAccountName=" + group, ctls);
				int count = 0;
				boolean isMember = false;
				while (answer != null && answer.hasMore()) {
					SearchResult sr = answer.next();
					groupDN = sr.getNameInNamespace();
					response = groupDN;
					Attribute thisAttr = null;
			   		NamingEnumeration<? extends Attribute> a = sr.getAttributes().getAll();
			   		while (a.hasMoreElements()) {
			   			thisAttr = a.nextElement();
			   			String thisId = thisAttr.getID();
			   			if (thisId.equals("member") || thisId.startsWith("member;")) {
			   				if (thisAttr.getAll().hasMoreElements()) {
			   					NamingEnumeration<?> member = thisAttr.getAll();
			   					while (member.hasMoreElements()) {
			   						String thisDN = member.nextElement().toString();
			   						if (userDN.equalsIgnoreCase(thisDN)) {
			   							isMember = true;
			   						}
			   					}
			   				}
						}
					}
					count++;
				}
				if (count == 0) {
					response = "Group Not Found";
				} else if (count > 1) {
					response = "Multiple group records found (" + count + ")";
				} else {
					if ("remove".equalsIgnoreCase(action)) {
						if (!isMember) {
							response = "User not a member of this group";
						}
					} else {
						if (isMember) {
							response = "User already a member of this group";
						}
					}
					i = baseGroupFilter.size();
				}
			}
			if (response.equals(groupDN)) {
				boolean isMember = false;
				Attribute thisAttr = userObject.getAttributes().get("memberOf");
				if (thisAttr != null && thisAttr.getAll().hasMoreElements()) {
					NamingEnumeration<?> member = thisAttr.getAll();
					while (member.hasMoreElements()) {
						String thisDN = member.nextElement().toString();
						if (groupDN.equalsIgnoreCase(thisDN)) {
							isMember = true;
						}
					}
				}
				if ("remove".equalsIgnoreCase(action)) {
					if (!isMember) {
						response = "User not a member of this group";
					}
				} else {
					if (isMember) {
						response = "User already a member of this group";
					}
				}
			}
		} catch (Exception e) {
			response = "Exception encountered searching for group " + group + ": " + e;
			log.error("Exception encountered searching for group " + group + ": " + e, e);
		}
		if (log.isDebugEnabled()) {
			log.debug("Response: " + response);
		}

		return response;
	}

	private void logActivity(HttpServletRequest req, JSONObject response, String action, String user, String group, String remoteAddr) {
		if (log.isDebugEnabled()) {
			log.debug("Logging service activity ....");
		}
        
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(AUDIT_SQL);
			ps.setString(1, action);
			ps.setString(2, user);
			ps.setString(3, group);
			ps.setInt(4, (Integer) response.get("responseCode"));
			ps.setString(5, (String) response.get("response"));
			ps.setString(6, req.getRemoteUser());
			ps.setString(7, remoteAddr);
			int rowCt = ps.executeUpdate();
			if (rowCt > 0) {
				if (log.isDebugEnabled()) {
					log.debug("Service activity successfully logged.");
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug("Unable to log service activity; action=" + action + "; user=" + user + "; group=" + group + "; response=" + response.toJSONString());
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered logging service activity: " + e.getMessage(), e);
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
}
