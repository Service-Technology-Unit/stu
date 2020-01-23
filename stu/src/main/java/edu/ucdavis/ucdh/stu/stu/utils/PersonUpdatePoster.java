package edu.ucdavis.ucdh.stu.stu.utils;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.up2date.beans.Update;
import edu.ucdavis.ucdh.stu.up2date.service.Up2DateService;

public class PersonUpdatePoster implements Serializable {
	private static final long serialVersionUID = 1;
	private String publisherId = null;
	private Up2DateService up2dateService = null;
	private DataSource dataSource = null;
	private List<Map<String,String>> field = new ArrayList<Map<String,String>>();
	private String tableName = null;
	private String fetchSql = null;
	private Log log = LogFactory.getLog(getClass());

	public PersonUpdatePoster(Up2DateService up2dateService, DataSource dataSource, String publisherId) {
		this.up2dateService = up2dateService;
		this.dataSource = dataSource;
		this.publisherId = publisherId;

		// fetch the publisher data from the Up2Date service
		Properties publisher = up2dateService.getPublisher(publisherId);
		if (publisher != null) {
			tableName = publisher.getProperty("tableName");
			String[] fieldList = publisher.getProperty("field").split(";");
			if (fieldList.length > 0) {
				for (int i=0; i<fieldList.length; i++) {
					Map<String,String> thisField = new HashMap<String,String>();
					String[] parts = fieldList[i].split(",");
					thisField.put("fieldName", parts[0]);
					thisField.put("columnName", parts[1]);
					field.add(thisField);
				}
				log.info("Publisher data obtained for publisher " + publisherId + "; data fields: " + field);
				fetchSql = "";
				String separator = "SELECT ";
				Iterator<Map<String,String>> i = field.iterator();
				while (i.hasNext()) {
					fetchSql += separator;
					fetchSql += i.next().get("columnName");
					separator = ", ";
				}
				fetchSql += " FROM ";
				fetchSql += tableName;
				fetchSql += " WHERE ID=?";
			} else {
				log.error("There are no source system fields on file for publisher " + publisherId + ".");
			}
		} else {
			log.error("There is no publisher on file with an ID of " + publisherId + ".");
		}
	}

	/**
	 * <p>Publishes the current values for the specified ID.</p>
	 *
	 * @param id the person id
	 * @param action the update action (add, change, or delete)
	 */
	public void publishPersonInfo(String id, String action) {
		if (log.isDebugEnabled()) {
			log.debug("Publishing " + action + " action for person #" + id);
		}

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(fetchSql);
			ps.setString(1, id);
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + fetchSql);
			}
			rs = ps.executeQuery();
			if (rs.next()) {
				if (log.isDebugEnabled()) {
					log.debug("Person #" + id + " found");
				}
				Properties properties = new Properties();
				Iterator<Map<String,String>> i = field.iterator();
				while (i.hasNext()) {
					Map<String,String> thisField = i.next();
					String fieldName = thisField.get("fieldName");
					String columnName = thisField.get("columnName");
					String fieldValue = rs.getString(columnName);
					if (StringUtils.isEmpty(fieldValue)) {
						fieldValue = "";
					}
					properties.setProperty(fieldName, fieldValue);
				}
				up2dateService.post(new Update(publisherId, action, properties));
			}
		} catch (SQLException e) {
			log.error("Exception encountered fetching data to publish for person #" + id + ": " + e.getMessage(), e);
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
	}
}
