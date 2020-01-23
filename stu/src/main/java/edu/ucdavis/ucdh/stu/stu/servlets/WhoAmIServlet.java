package edu.ucdavis.ucdh.stu.stu.servlets;

import java.util.Iterator;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import edu.ucdavis.ucdh.stu.stu.beans.Contact;

/**
 * <p>This servlet returns a JSON object containing details of the currently authenticated User.</p>
 */
public class WhoAmIServlet extends JavascriptServlet {
	private static final long serialVersionUID = 1;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		servletPath = "/whoami.js";
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
		JSONObject whoami = new JSONObject();

		String remoteAddr = req.getRemoteAddr();
		if (StringUtils.isNotEmpty(req.getHeader("X-Forwarded-For"))) {
			remoteAddr = req.getHeader("X-Forwarded-For");
		}
		Contact user = (Contact) req.getSession().getAttribute(USER_DETAILS);
		if (user != null) {
			whoami.put("sys_id", user.getSysId());
			whoami.put("firstName", user.getFirstName());
			whoami.put("middleName", user.getMiddleName());
			whoami.put("lastName", user.getLastName());
			whoami.put("name", user.getFirstName() + " " + user.getLastName());
			whoami.put("email", user.getEmail());
			whoami.put("phone", user.getPhoneNr());
			whoami.put("cell", user.getCellPhone());
			whoami.put("pager", user.getPager());
			whoami.put("pagerProvider", user.getPagerProvider());
			whoami.put("deptId", user.getDepartmentId());
			whoami.put("deptName", user.getDepartment());
			whoami.put("supervisor", user.getSupervisor());
			whoami.put("manager", user.getManager());
			whoami.put("remoteAddr", remoteAddr);
			whoami.put("remoteHost", remoteAddr);
			whoami.put("remoteUser", user.getId());
			if (user.getGroup() != null && user.getGroup().size() > 0) {
				JSONArray groups = new JSONArray();
				Iterator<String> i = user.getGroup().iterator();
				while (i.hasNext()) {
					groups.add(i.next());
				}
				whoami.put("groups", groups);
			}
		}

		return whoami.toJSONString();
	}
}
