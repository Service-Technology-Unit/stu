package edu.ucdavis.ucdh.stu.stu.servlets;

import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.imgscalr.Scalr;
import org.springframework.web.context.support.WebApplicationContextUtils;

import edu.ucdavis.ucdh.stu.stu.beans.Contact;
import edu.ucdavis.ucdh.stu.stu.utils.MD5Util;

/**
 * <p>This servlet delivers an Avatar based on the contact photo.</p>
 */
public class AvatarServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private static final String USER_DETAILS = "userDetails";
	private static final String GRAVATAR_BASE_URL = "https://s.gravatar.com/avatar/";
	private static final String GRAVATAR_OPTIONS = "?d=404";
	private Log log = LogFactory.getLog(getClass());
	private byte[] technicalErrorImage = null;
	private byte[] notAuthorizedImage = null;
	private byte[] notFoundImage = null;
	private byte[] permissionWithheldImage = null;
	private Map<String,byte[]> gravatars = null;
	private Map<String,byte[]> photos = null;
	private DataSource serviceManagerDataSource = null;
	private DataSource badgeDataSource = null;
	private DataSource portraitDataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	@SuppressWarnings("unchecked")
	public void init() throws ServletException {
		log.info("Initializing AvatarServlet ...");
		ServletConfig config = getServletConfig();
		gravatars = (Map<String,byte[]>) config.getServletContext().getAttribute("AvatarServletGravatars");
		if (gravatars == null) {
			log.info("Establishing new gravatar cache ...");
			gravatars = new HashMap<String,byte[]>();
			config.getServletContext().setAttribute("AvatarServletGravatars", gravatars);
		}
		photos = (Map<String,byte[]>) config.getServletContext().getAttribute("AvatarServletPhotos");
		if (photos == null) {
			log.info("Establishing new photo cache ...");
			photos = new HashMap<String,byte[]>();
			config.getServletContext().setAttribute("AvatarServletPhotos", photos);
		}
		serviceManagerDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("dataSource");
		badgeDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("badgeDataSource");
		portraitDataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("portraitDataSource");
		technicalErrorImage = fetchImageDataFromURL((String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("technicalErrorImage"));
		notAuthorizedImage = fetchImageDataFromURL((String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("notAuthorizedImage"));
		notFoundImage = fetchImageDataFromURL((String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("notFoundImage"));
		permissionWithheldImage = fetchImageDataFromURL((String) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("permissionWithheldImage"));
		log.info("AvatarServlet initialized.");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		if ("true".equals(req.getParameter("clearCache"))) {
			log.info("Clearing image cache as directed ...");
			gravatars = new HashMap<String,byte[]>();
			req.getSession().getServletContext().setAttribute("AvatarServletGravatars", gravatars);
			photos = new HashMap<String,byte[]>();
			req.getSession().getServletContext().setAttribute("AvatarServletPhotos", photos);
			log.info("Image cache cleared.");
		}

		byte[] imageData = notFoundImage;
		String userId = getIdFromUrl(req, "/avatar/");
		if (StringUtils.isNotEmpty(userId)) {
			Map<String,String> user = getUserData(userId);
			if (user != null) {
				imageData = getGravatar(userId, user.get("email"));
				if (imageData == null) {
					Contact userDetails = (Contact) req.getSession().getAttribute(USER_DETAILS);
					if (userDetails != null) {
						if ("t".equalsIgnoreCase(userDetails.getItStaff()) || userId.equalsIgnoreCase(userDetails.getId())) {
							imageData = getAvatar(req, user);
							if (imageData == null) {
								imageData = notFoundImage;
							}
						} else {
							imageData = notAuthorizedImage;
						}
					} else {
						imageData = notAuthorizedImage;
					}
				}
			}
		}

		res.setContentType("image/jpg");
		res.setHeader("Cache-Control", "private");			
		res.setHeader("Pragma", "private");			
		ServletOutputStream out = null;
		BufferedOutputStream bos = null;
		try {
			out = res.getOutputStream();
			bos = new BufferedOutputStream(out);
			bos.write(imageData, 0, imageData.length);    
		} catch (Exception e) {
			log.warn("Exception encountered attempting to send avatar; sending error image instead", e);
			imageData = technicalErrorImage;
			try {
				bos = new BufferedOutputStream(out);
				bos.write(imageData, 0, imageData.length);    
			} catch (Exception e1) {
				log.error("Exception encountered attempting to send error image", e1);
			}
		} finally {
			if (bos != null) {
				bos.close();
			}
			if (out != null) {
				out.close();
			}
		}
	}

	/**
	 * <p>The Servlet "doPost" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		res.setContentType("image/jpg");
		res.setHeader("Cache-Control", "private");			
		res.setHeader("Pragma", "private");			
		ServletOutputStream out = null;
		BufferedOutputStream bos = null;
		try {
			out = res.getOutputStream();
			bos = new BufferedOutputStream(out);
			bos.write(technicalErrorImage, 0, technicalErrorImage.length);    
		} catch (Exception e) {
			log.error("Exception encountered attempting to error image", e);
		} finally {					
			if (bos != null) {
				bos.close();
			}
			if (out != null) {
				out.close();
			}
		}
	}

	/**
	 * <p>Returns the user data associated with passed contact id.</p>
	 *
	 * @param id the contact id
	 * @return the user data associated with passed contact id
	 */
	private Map<String,String> getUserData(String userId) {
		Map<String,String> user = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching user data from contact manager database ...");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = serviceManagerDataSource.getConnection();
			ps = conn.prepareStatement("select user_id, email, ucd_protect_photo from contctsm1 where active='t' and contact_name=?");
			ps.setString(1, userId);
			rs = ps.executeQuery();
			if (rs.next()) {
				user = new HashMap<String,String>();
				user.put("id", userId);
				user.put("ppsid", rs.getString("USER_ID"));
				user.put("email", rs.getString("EMAIL"));
				user.put("confidential", rs.getString("UCD_PROTECT_PHOTO"));
				if (log.isDebugEnabled()) {
					log.debug("ppsid: " + user.get("ppsid"));
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered accessing user data: " + e.getMessage(), e);
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

		return user;
	}

	/**
	 * <p>Returns the byte array of the image data.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the byte array of the image data
	 */
	private byte[] getGravatar(String userId, String email) {
		byte[] imageData = null;

		if (gravatars.containsKey(userId)) {
			imageData = gravatars.get(userId);
		} else {
			if (StringUtils.isNotEmpty(email)) {
				email = email.trim();
				email = email.toLowerCase();
				email = MD5Util.md5Hex(email);
				imageData = fetchImageDataFromURL(GRAVATAR_BASE_URL + email + GRAVATAR_OPTIONS);
				if (imageData != null) {
					gravatars.put(userId, imageData);
				}
			}
		}

		return imageData;
	}

	/**
	 * <p>Returns the byte array of the image data.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @return the byte array of the image data
	 */
	private byte[] getAvatar(HttpServletRequest req, Map<String,String> user) {
		byte[] imageData = null;

		String id = user.get("id");
		String ppsid = user.get("ppsid");
		String confidential = user.get("confidential");
		if (StringUtils.isNotEmpty(user.get("ppsid"))) {
			if ("f".equalsIgnoreCase(confidential)) {
				if (photos.containsKey(id)) {
					imageData = photos.get(id);
				} else {
					String photoId = getPhotoId(ppsid);
					if (StringUtils.isNotEmpty(photoId)) {
						imageData = fetchImageFileData(photoId);
						photos.put(id, imageData);
					}
				}
			} else {
				imageData = permissionWithheldImage;
			}
		}

		return imageData;
	}

	/**
	 * <p>Returns the photo id associated with passed ppsid.</p>
	 *
	 * @param id the ppsid
	 * @return the photo id associated with passed ppsid
	 */
	private String getPhotoId(String id) {
		String photoId = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching photoId from badge system ...");
		}
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = badgeDataSource.getConnection();
			ps = conn.prepareStatement("select c_id from dbo.cardholder where c_nick_name=?");
			ps.setString(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				photoId = rs.getString(1);
				if (log.isDebugEnabled()) {
					log.debug("photoId: " + photoId);
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered accessing photoId: " + e.getMessage(), e);
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

		return photoId;
	}

	/**
	 * <p>Returns the byte array of the image data.</p>
	 *
	 * @param id the id of the photo data record
	 * @return the byte array of the image data
	 */
	private byte[] fetchImageFileData(String id) {
		byte[] imageData = null;

		if (log.isDebugEnabled()) {
			log.debug("Fetching image data ...");
		}
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = portraitDataSource.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery("select pt_image from portrait where pt_cardholder_id=" + id);
			if (rs.next()) {
				imageData = rs.getBytes(1);
				if (log.isDebugEnabled()) {
					log.debug("original image size: " + imageData.length);
				}
				imageData = resizeImage(imageData);
				if (log.isDebugEnabled()) {
					log.debug("resized image size: " + imageData.length);
				}
			}
		} catch (SQLException e) {
			log.error("Exception encountered processing image data: " + e.getMessage(), e);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					//
				}
			}
			if (stmt != null) {
				try {
					stmt.close();
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

		return imageData;
	}

	/**
	 * <p>Returns a new byte array of the resized image data.</p>
	 *
	 * @param imageData the original image data
	 * @return the byte array of the resized image data
	 */
	private byte[] resizeImage(byte[] imageData) {
		InputStream in = new ByteArrayInputStream(imageData);
		BufferedImage image = null;
		BufferedImage avatar = null;
		try {
			image = ImageIO.read(in);
			log.debug("Image: " + image.getHeight() + "x" + image.getWidth());
			avatar = Scalr.resize(image, Scalr.Method.SPEED, Scalr.Mode.FIT_TO_HEIGHT, 70, 80, Scalr.OP_ANTIALIAS);
			log.debug("Avatar: " + avatar.getHeight() + "x" + avatar.getWidth());
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ImageIO.write(avatar, "jpg", out);
			out.flush();
			imageData = out.toByteArray();
			out.close();
		} catch (IOException e) {
			log.error("Exception encountered resizing image", e);;
		}

		return imageData;
	}

	/**
	 * <p>Returns the byte array of the image data.</p>
	 *
	 * @param url the url of the image file
	 * @return the byte array of the image data
	 */
	private byte[] fetchImageDataFromURL(String url) {
		byte[] imageData = null;

		if (log.isDebugEnabled()) {
			log.debug("Retrieving image from URL " + url);
		}
		try {
			HttpClient client = HttpClients.custom().setSSLHostnameVerifier(new NoopHostnameVerifier()).build();
			HttpGet get = new HttpGet(url);
			get.setHeader("User-Agent", "Java Servlet");
			HttpResponse res = client.execute(get);
			int responseCode = res.getStatusLine().getStatusCode();
			imageData = EntityUtils.toByteArray(res.getEntity());
			if (responseCode == 200) {
				if (log.isDebugEnabled()) {
					log.debug(imageData.length + " byte image returned from URL " + url);
				}
			} else {
				imageData = null;
				if (responseCode == 404) {
					if (log.isDebugEnabled()) {
						log.debug("The image from URL \"" + url + "\" was not found.");
					}
				} else {
					log.error("Invalid response from URL " + url + ": " + responseCode);
				}
			}
		} catch (Exception e) {
			log.error("Exception occurred processing URL " + url + ": " + e, e);
		}

		return imageData;
	}

	/**
	 * <p>Returns the id present in the URL, if any.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param servletPath the path to the servlet, including both the leading and trailing "/"
	 * @return the id present in the URL, if any
	 */
	private static String getIdFromUrl(HttpServletRequest req, String servletPath) {
		String id = null;

		String basePath = req.getContextPath() + servletPath;
		if (req.getRequestURI().length() > basePath.length()) {
			id = req.getRequestURI().substring(basePath.length());
			if (id.indexOf(".") != -1) {
				id = id.substring(0, id.indexOf("."));
			}
			id = id.toLowerCase();
		}

		return id;
	}
}
