package edu.ucdavis.ucdh.stu.stu.servlets;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.web.context.support.WebApplicationContextUtils;

/**
 * <p>This servlet performs a number of standard SQL functions and reports the elapsed time in XML format.</p>
 */
public class PerformanceMonitorServlet extends HttpServlet {
	private static final long serialVersionUID = 1;
	private static final int INSERT_COUNT = 1000;
	private static final String ID_LIST = "idList";
	private static final String CLEAN_SQL = "DELETE FROM PM_PERFMON";
	private static final String INSERT_SQL = "INSERT INTO PM_PERFMON (CHAR_FIELD, INTEGER_FIELD, DATE_FIELD, DATETIME_FIELD, VARCHAR_FIELD, TEXT_FIELD, SYSMODTIME, SYSMODCOUNT, SYSMODUSER) VALUES(?, ?, ?, ?, ?, ?, GETDATE(), 0, 'PerformanceMonitorServlet')";
	private static final String READ_SQL = "SELECT * FROM PM_PERFMON ORDER BY ID";
	private static final String UPDATE_SQL = "UPDATE PM_PERFMON SET CHAR_FIELD=?, INTEGER_FIELD=?, DATE_FIELD=?, DATETIME_FIELD=?, VARCHAR_FIELD=?, TEXT_FIELD=?, SYSMODTIME=GETDATE(), SYSMODCOUNT=SYSMODCOUNT+1, SYSMODUSER='PerformanceMonitorServlet' WHERE ID=?";
	private static final String DELETE_SQL = "DELETE FROM PM_PERFMON WHERE ID=?";
	private static final String SAMPLE_TEXT = "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nullam nec urna nibh, sit amet facilisis tellus. Fusce elementum eleifend velit et iaculis. Duis porttitor erat volutpat arcu feugiat molestie. Ut nisl tortor, rhoncus semper pulvinar non, mattis vel libero. Praesent luctus, dui ut dictum accumsan, nibh tortor dapibus mi, id tristique dui lacus ut lorem. Sed congue tortor eu lectus vestibulum semper. Quisque laoreet, arcu quis facilisis facilisis, sem lacus gravida urna, quis vestibulum ante neque sit amet ante. Donec quis auctor odio. Praesent hendrerit tortor eget orci accumsan vehicula. Morbi eu tellus mauris, eget vestibulum elit. Integer iaculis elementum rhoncus. Proin id lorem magna, eget imperdiet metus. Phasellus at lacus eu mi ultrices porta eu ornare metus. Phasellus eu magna nibh, in condimentum odio. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.\n\nCras pharetra ligula vitae mauris pretium vel pulvinar risus dignissim. Praesent eget massa vitae justo sagittis tincidunt id sit amet magna. Morbi sem leo, tempor ac accumsan non, hendrerit at enim. Sed ut viverra velit. Praesent elit tortor, pellentesque sed vestibulum luctus, luctus eu tellus. Ut pulvinar bibendum risus sed porttitor. Quisque nisl quam, sagittis sit amet imperdiet nec, pretium non dolor. Quisque et pulvinar lectus. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Proin consequat vestibulum nibh vel commodo. Cras in enim vitae ante placerat bibendum. Quisque egestas sodales mi, at rhoncus metus tempor a. Nunc viverra lorem id velit sagittis pretium. Aenean libero enim, pretium quis feugiat at, aliquam sit amet enim. Praesent malesuada mi at dolor fermentum sed condimentum felis vulputate. Nam vitae porttitor libero.\n\nUt at magna leo, ut aliquet orci. Maecenas vel tellus sapien. Morbi facilisis nibh at lorem tincidunt faucibus. Nam ac lectus ante. Vestibulum et cursus elit. Donec faucibus, felis a volutpat porta, dolor mi lobortis metus, eget vehicula orci urna vitae quam. Sed semper, mi eget interdum mattis, nisi quam elementum ipsum, nec lobortis diam odio id urna. Duis eu nisl nibh, sed vestibulum nisi. Suspendisse sed enim augue, eget varius magna.\n\nPellentesque suscipit purus sit amet orci placerat vel suscipit nibh feugiat. Donec sem justo, sagittis nec iaculis sed, lobortis in augue. Duis vulputate nisi libero. Praesent velit elit, rutrum sit amet sodales eu, elementum sit amet lectus. Ut luctus suscipit mauris, a malesuada odio dignissim in. Phasellus eu nisi in lacus pharetra blandit sed ac odio. Aliquam vulputate iaculis nisi eu pulvinar. Aliquam eget justo nisi. Aenean nec diam ut neque convallis ullamcorper euismod vel elit. Integer ultricies tincidunt magna non lacinia. Sed ultrices dictum leo. Vivamus massa velit, eleifend ut suscipit ut, dignissim ac magna. Aenean quis dolor at felis aliquam feugiat. Cras quis nisi at nibh suscipit vestibulum eget sit amet felis. Proin sed felis diam.\n\nPraesent et neque purus. Donec vestibulum pulvinar nunc at bibendum. Donec a felis sit amet ligula vestibulum luctus eu vitae odio. Donec lobortis congue mi scelerisque vehicula. Vivamus vel sapien et tellus consectetur malesuada. Nullam nec enim lacus. Maecenas eget augue et ipsum interdum sollicitudin. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Aenean sit amet elit vitae sapien varius luctus. Suspendisse potenti. Sed nisi turpis, sodales vitae facilisis vitae, aliquam ut arcu. Aliquam erat volutpat. Aenean ut turpis augue, ut volutpat nibh. Proin purus diam, iaculis non venenatis at, tincidunt et lorem. In hac habitasse platea dictumst.\n\nMorbi tempus justo quis ante pulvinar scelerisque. Etiam at ante urna, nec eleifend urna. Morbi commodo, lorem sed imperdiet blandit, quam nisl congue nisl, nec commodo mi risus et neque. In vel ligula semper erat tristique tincidunt. In enim sapien, lacinia sit amet feugiat non, ullamcorper eget elit. Integer mattis mollis eros, laoreet fringilla neque scelerisque eget. Donec a justo risus.\n\nDonec feugiat ullamcorper lorem, vitae consectetur lacus tempus vitae. Duis nec mauris nisi. Cras elementum, odio eu blandit egestas, lacus massa vulputate sem, sed venenatis orci metus a diam. Morbi rhoncus venenatis felis eget dignissim. Sed fermentum lorem in ipsum euismod sed sollicitudin lacus vehicula. Vivamus a velit erat, eu aliquam libero. Ut id vehicula felis. Nunc vel tortor non mauris porttitor iaculis. Aenean a mollis magna.\n\nPhasellus tincidunt scelerisque semper. Morbi eget diam dolor, ac iaculis libero. Integer ac ligula sapien, a interdum ligula. In eget sem dolor, et ullamcorper nunc. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Ut justo quam, sollicitudin nec convallis egestas, gravida nec erat. Nullam tempus fringilla aliquam. Maecenas pulvinar erat a tellus tristique luctus. Vestibulum hendrerit est ut ipsum hendrerit iaculis ullamcorper ligula vestibulum. In hac habitasse platea dictumst. Ut tempus lorem eu eros semper vitae vestibulum enim posuere. Sed lobortis, odio non bibendum bibendum, felis metus venenatis lorem, et consequat ligula nisi non leo. Maecenas imperdiet porttitor porttitor. Vivamus non ipsum ut lacus auctor sollicitudin sed non dui.\n\nEtiam eleifend tellus vitae diam lacinia vel facilisis urna ullamcorper. Duis nibh sem, facilisis quis volutpat quis, aliquam in arcu. Fusce metus est, vestibulum eu tincidunt quis, mattis vitae elit. Nam felis leo, vehicula in tincidunt sit amet, volutpat id eros. Integer ut lacus ac massa rhoncus elementum. Sed egestas consequat mi, sed dictum ligula mattis in. Suspendisse dignissim diam porta augue faucibus suscipit. Praesent malesuada augue eget orci tempus volutpat. Etiam nisl arcu, sodales sit amet pharetra eu, suscipit id purus. Fusce vehicula aliquam nunc quis fringilla. Etiam porta commodo nibh, non aliquam mi egestas a. Nunc in leo ligula, eu pellentesque tortor.\n\nFusce euismod, magna et placerat sollicitudin, purus mi malesuada massa, eu sagittis risus nisi eget felis. Mauris sit amet turpis ut nisi pulvinar semper. Donec venenatis interdum lacus vitae venenatis. Sed hendrerit volutpat ipsum vel dapibus. Sed cursus gravida adipiscing. Sed adipiscing orci eu augue aliquam sed eleifend est fringilla. Proin mattis tortor eu ipsum accumsan sodales. Mauris pellentesque, enim eget lacinia feugiat, massa leo ultrices justo, eu rhoncus libero elit vitae purus. Donec eu augue lacus, vitae interdum ipsum. Aliquam sed ipsum eu ipsum blandit euismod. Nam fringilla rutrum luctus.\n\nQuisque sagittis adipiscing ante sed mattis. Nullam at eleifend mi. Sed suscipit, elit eget scelerisque sollicitudin, libero nibh sollicitudin odio, vel aliquam purus diam et odio. Cras felis neque, faucibus et ornare non, ultrices rutrum tortor. Nullam luctus congue sapien, id malesuada mi scelerisque non. Nullam mollis urna vel orci imperdiet malesuada. Donec fermentum, augue vel tempus aliquet, nibh diam dictum orci, ullamcorper laoreet nisi tellus vel sapien. Sed viverra auctor odio, quis vulputate purus tincidunt nec. Nullam sagittis interdum dolor, eu vehicula metus fringilla sit amet. Integer purus ipsum, malesuada ac facilisis et, aliquam a felis. Proin vitae mi eu felis ullamcorper eleifend ut eget libero. Integer a tellus in leo dictum placerat. Fusce ac tellus est, at hendrerit turpis. Nunc sed massa felis, sit amet scelerisque augue. In erat elit, pulvinar at suscipit sit amet, varius at metus. Aliquam blandit vestibulum mauris, dapibus pellentesque nulla laoreet id.\n\nEtiam congue magna laoreet tortor pretium condimentum. Pellentesque elementum fermentum magna, in malesuada turpis laoreet sit amet. Donec ipsum lacus, molestie ut pharetra quis, vulputate a neque. Nunc id adipiscing massa. Cras ultrices, nisl porta bibendum placerat, metus lacus porta lectus, sit amet commodo augue metus et tellus. Etiam ac libero erat, eu adipiscing urna. Sed lacinia convallis ipsum, ut pellentesque sapien tristique quis. Donec interdum velit a arcu luctus mattis. Integer et laoreet enim. Aenean id quam libero. Aliquam a tempus ligula. Vivamus porta lectus in odio tincidunt vel varius tortor dictum.\n\nPraesent dignissim, mi et semper cursus, lorem nibh fringilla leo, nec ornare neque nulla vel metus. Etiam ultricies aliquam tortor, id venenatis velit egestas nec. Morbi pulvinar nisi sit amet magna pharetra sodales. Nulla massa orci, lobortis in tincidunt sit amet, hendrerit sit amet turpis. Integer ultricies, quam sed malesuada pellentesque, erat nisi ultricies tortor, a aliquet ante neque id risus. Nullam semper purus vitae justo pulvinar at eleifend dolor faucibus. Nulla pharetra nisi ac sapien lobortis viverra. In dictum vehicula augue non posuere. Suspendisse potenti. Etiam a posuere felis.\n\nAliquam sit amet varius dolor. Donec porta ipsum nec mauris consequat nec blandit nunc porta. Ut laoreet metus elit. Etiam imperdiet sodales pretium. Quisque eget lacus enim. Sed fermentum nibh ac libero feugiat vehicula dictum orci vulputate. Proin suscipit nulla at lectus pharetra at venenatis mauris eleifend. Suspendisse ac tristique purus. Mauris fermentum ipsum vitae magna tempor imperdiet. In erat sapien, scelerisque non sagittis vitae, aliquam nec erat. Phasellus sit amet justo enim, vitae elementum nulla.\n\nVestibulum nulla sapien, posuere id mattis in, molestie vel ante. Vestibulum condimentum bibendum leo a malesuada. Mauris sed lacus eget orci consequat ultrices ut vel magna. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque luctus iaculis nisi, id condimentum lorem luctus eget. Nunc massa nisi, vulputate sit amet hendrerit id, imperdiet non nunc. Etiam quis nunc id ligula feugiat varius vitae non mi. Nulla eu dapibus ligula. Aenean imperdiet pellentesque lectus ultrices rhoncus. Quisque dignissim dapibus dictum. Maecenas congue odio a turpis rhoncus tincidunt. Phasellus id leo suscipit dui fringilla aliquet. Donec et luctus arcu.\n\nMorbi gravida cursus dapibus. Proin eget sollicitudin ipsum. Curabitur a sapien neque, quis sagittis diam. Duis vehicula nunc nec lorem ultricies non vehicula lectus ultrices. Cras at arcu nec diam feugiat laoreet eu vel lorem. Donec iaculis lacinia elit, commodo mattis odio aliquam at. Phasellus sollicitudin pharetra elit, eget auctor ligula congue a. Sed non placerat purus. Morbi nec risus arcu. Suspendisse eu sem vel massa facilisis mattis. Fusce adipiscing aliquam venenatis. Fusce lacinia est in orci gravida a eleifend ante fringilla.\n\nVestibulum vel dictum lacus. Donec dignissim ultricies lorem non congue. Nullam sodales, massa ac porta pellentesque, leo lacus molestie justo, id cursus lectus arcu vitae augue. Nunc tincidunt tempus lacinia. Etiam dictum, ante non fermentum consectetur, turpis odio aliquam ante, sit amet commodo diam lorem nec sem. Ut a velit vitae metus tristique ornare rhoncus cursus sem. Ut iaculis metus elit, vitae scelerisque urna. Phasellus sit amet lectus quis massa aliquet lobortis. Quisque nibh lorem, aliquam a mollis non, condimentum sit amet eros.\n\nUt ac aliquam nisl. Ut pretium bibendum porttitor. Vestibulum mauris ante, luctus sit amet facilisis non, mattis vitae leo. Vestibulum eu tortor a metus commodo ornare et id elit. Phasellus varius sem et est aliquam vel placerat elit iaculis. Integer interdum pulvinar pretium. In id massa massa. Aenean elit risus, eleifend eu fringilla id, accumsan tempus nibh. Pellentesque fringilla ligula scelerisque turpis faucibus consectetur. Nunc in massa lectus, id auctor nulla. Quisque vehicula magna sed elit lobortis vitae ornare lacus laoreet. Nullam accumsan lorem dapibus lectus varius a consectetur sapien eleifend. Pellentesque facilisis, nisl in malesuada placerat, sem justo cursus felis, eu ultrices massa magna at elit. Aliquam eget enim dui, a ullamcorper erat.\n\nInteger vehicula, elit non pulvinar convallis, nisl diam cursus nulla, ac facilisis tortor ante vitae est. Mauris volutpat arcu a tellus molestie porttitor. Nulla ornare, lectus at cursus pharetra, mi libero suscipit turpis, et aliquet nulla felis eu lacus. Donec vel congue nunc. Vivamus quis odio in sapien suscipit bibendum a a turpis. Mauris congue mauris sit amet magna viverra vel rutrum tellus sagittis. Morbi sit amet arcu elit, sagittis placerat tortor. Sed eget ante urna. Mauris nibh elit, viverra ut mollis ut, tempor luctus velit. Pellentesque vehicula malesuada mattis. Duis elementum mattis placerat. Aenean congue suscipit sem. Duis venenatis tortor et metus consectetur volutpat in id nulla. In at justo vel quam pretium feugiat in ac nisl. Ut orci lorem, convallis eu venenatis in, volutpat in leo.\n\nQuisque vulputate aliquet faucibus. Phasellus eleifend porta dui, in tempus ipsum hendrerit a. Morbi fringilla dui quis lectus porta scelerisque. In et porttitor felis. Vivamus ac velit nisi, eu gravida massa. Phasellus magna purus, accumsan ac dictum sed, faucibus et sapien. Nunc nunc risus, dictum a elementum vel, adipiscing consectetur enim. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia Curae; Suspendisse eget elementum purus.\n\nCum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Vivamus ullamcorper, ipsum id venenatis placerat, velit risus mattis lacus, sed pellentesque est augue id sapien. Pellentesque magna enim, porttitor vitae euismod quis, cursus non lacus. Donec et felis ipsum. Sed eget mi a est dignissim porttitor. Vivamus laoreet dolor ac risus ullamcorper condimentum. Aenean condimentum pharetra arcu ut semper. Vestibulum ullamcorper, lorem ac pretium facilisis, mauris nulla ultricies magna, vitae aliquam nisi dui a purus. Quisque vel bibendum mi. Sed scelerisque pellentesque aliquet. Duis elit tortor, euismod non gravida sit amet, dignissim ut erat. Integer ac quam ante, non consequat risus.\n\nDonec nec mauris tellus. Pellentesque quis mi justo. Nam tellus lacus, congue iaculis molestie id, egestas at mi. Integer dapibus odio a quam venenatis a cursus sapien vehicula. Morbi mollis felis quis felis viverra tempor vulputate risus accumsan. Duis pulvinar, risus in venenatis facilisis, nibh purus interdum sapien, quis posuere dui sapien et velit. Integer mi ipsum, pulvinar non ornare sit amet, rutrum eu elit. Duis a risus sit amet orci molestie egestas. Ut luctus elementum massa, at sollicitudin dolor volutpat sed. Praesent aliquet vehicula interdum. Maecenas elementum consectetur mi, vel rutrum dui imperdiet quis.\n\nMauris pretium diam id dolor auctor ut ullamcorper augue euismod. Ut iaculis rutrum orci suscipit vehicula. Fusce quis magna nisl, at dapibus ante. Phasellus eleifend vulputate nisl a elementum. Donec bibendum tincidunt risus et bibendum. Nulla nec eros quis diam consequat ornare. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus. Duis sed felis vitae ante mollis placerat rutrum sit amet leo. Morbi consectetur ultricies justo, vitae placerat neque semper vitae. Nulla vel nulla massa, auctor feugiat augue. Nunc ullamcorper magna quis felis lacinia venenatis. Aliquam pretium elit at justo ultrices aliquet.\n\nInteger vehicula nulla vitae dolor pharetra volutpat. Nullam vitae quam erat, ac dapibus turpis. Praesent sed augue ligula, ut tempor augue. Quisque at orci odio, nec gravida lorem. Mauris fringilla venenatis enim at suscipit. Donec ut mauris ipsum. Morbi vel lectus ligula, a elementum velit. Sed id tellus nec justo suscipit cursus ac at leo. Maecenas fermentum pellentesque sollicitudin. Nulla tempor dui id orci vulputate mollis. Maecenas convallis ultrices sollicitudin. Duis placerat scelerisque tortor, condimentum vestibulum risus ullamcorper id. Pellentesque vitae diam sit amet libero ornare porta vitae in ligula. Proin dignissim tortor in urna condimentum quis gravida tellus fringilla. Praesent tincidunt sem vitae arcu commodo accumsan. Nunc in magna nibh, id feugiat eros.\n\nSed sit amet lorem dapibus nisi bibendum condimentum. Aenean eget nibh eget dui accumsan adipiscing quis nec tellus. Donec consectetur nisl id mi consequat vehicula. Aenean et massa risus, ac ultrices turpis. Curabitur pellentesque, tellus sit amet tempus rhoncus, odio sapien venenatis libero, at sagittis lectus erat a velit. Ut ut turpis purus. Suspendisse tempus arcu ut libero bibendum in tristique nunc tristique. Nunc ante lacus, varius quis feugiat in, luctus in sem. Aliquam quis augue lectus. In hac habitasse platea dictumst. Vestibulum quis rutrum turpis. Sed lacus elit, dapibus et porta ac, dignissim ultricies leo. Fusce id nisi porttitor ipsum faucibus tristique ut eu turpis.";
	private static final Random RANDOM = new Random();
	private Log log = LogFactory.getLog(getClass());
	private DataSource dataSource = null;

	/**
	 * @inheritDoc
	 * @see javax.servlet.Servlet#init(javax.servlet.ServletConfig)
	 */
	public void init() throws ServletException {
		super.init();
		ServletConfig config = getServletConfig();
		dataSource = (DataSource) WebApplicationContextUtils.getRequiredWebApplicationContext(config.getServletContext()).getBean("utilDataSource");
	}

	/**
	 * <p>The Servlet "GET" method.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
		res.setCharacterEncoding("UTF-8");
		res.setContentType("text/xml;charset=UTF-8");
		res.getWriter().write(processRequest(req, res));
    }

	/**
	 * <p>The Servlet "doPost" method -- this method is not supported in this
	 * servlet.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @throws IOException 
	 */
	public void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
		sendError(req, res, HttpServletResponse.SC_METHOD_NOT_ALLOWED, "The POST method is not allowed for this URL");
	}

	/**
	 * <p>Returns the XML response.</p>
	 *
	 * @param req the <code>HttpServletRequest</code> object
	 * @param res the <code>HttpServletResponse</code> object
	 * @return the Javascript response
	 */
	private String processRequest(HttpServletRequest req, HttpServletResponse res) {
		String response = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n";

		response += doStart();
		response += doClean();
		response += doInsert();
		response += doRead(req);
		response += doUpdate(req);
		response += doDelete(req);
		response += doEnd();

		return response;
	}

	private String doStart() {
		if (log.isDebugEnabled()) {
			log.debug("Starting real-time performance monitoring.");
		}
		String response = "<response>\n";
		response += " <start>" + getTime() + "</start>\n";
		return response;
	}

	private String doClean() {
		if (log.isDebugEnabled()) {
			log.debug("Cleaning out database table.");
		}
		String response = " <clean>\n";
		response += markStart("clean");
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + CLEAN_SQL);
			}
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(CLEAN_SQL);
			int rowCt = ps.executeUpdate();
			if (log.isDebugEnabled()) {
				log.debug(rowCt + " rows affected by this operation");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to clean out the database: " + e.getMessage(), e);
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
		response += markEnd("clean");
		response += " </clean>\n";
		return response;
	}

	private String doInsert() {
		if (log.isDebugEnabled()) {
			log.debug("Inserting " + INSERT_COUNT + " records into the database table.");
		}
		String response = " <insert>\n";
		response += markStart("insert");
		Connection conn = null;
		PreparedStatement ps = null;
		int rowsInserted = 0;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + INSERT_SQL);
			}
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(INSERT_SQL);
			for (int i=0; i<INSERT_COUNT; i++) {
				String charField = "t";
				int x = i/3;
				x = i - x*3;
				if (x > 1) {
					charField = "f";
				}
				ps.setString(1, charField);
				ps.setInt(2, RANDOM.nextInt(999999999));
				ps.setDate(3, new java.sql.Date(new Date().getTime() - RANDOM.nextInt(999999999)));
				ps.setTimestamp(4, new Timestamp(new Date().getTime() + RANDOM.nextInt(999999999)));
				ps.setString(5, SAMPLE_TEXT.substring(0, RANDOM.nextInt(499)));
				ps.setString(6, SAMPLE_TEXT.substring(0, RANDOM.nextInt(16000)));
				rowsInserted += ps.executeUpdate();
			}
			if (log.isDebugEnabled()) {
				log.debug(rowsInserted + " rows affected by this operation");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to insert records into the database: " + e.getMessage(), e);
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
		response += markEnd("insert");
		response += " </insert>\n";
		return response;
	}

	private String doRead(HttpServletRequest req) {
		if (log.isDebugEnabled()) {
			log.debug("Reading all records in the database table.");
		}
		List<Integer> id = new ArrayList<Integer>();
		String response = " <read>\n";
		response += markStart("read");
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		int readCt = 0;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + READ_SQL);
			}
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(READ_SQL);
			rs = ps.executeQuery();
			while (rs.next()) {
				id.add(rs.getInt("id"));
				readCt++;
			}
			if (log.isDebugEnabled()) {
				log.debug(readCt + " records read.");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to read from the database: " + e.getMessage(), e);
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
		req.setAttribute(ID_LIST, id);
		response += markEnd("read");
		response += " </read>\n";
		return response;
	}

	private String doUpdate(HttpServletRequest req) {
		@SuppressWarnings("unchecked")
		List<Integer> id = (List<Integer>) req.getAttribute(ID_LIST);
		if (log.isDebugEnabled()) {
			log.debug("Updating " + id.size() + " records in the database table.");
		}
		String response = " <update>\n";
		response += markStart("update");
		Connection conn = null;
		PreparedStatement ps = null;
		int updateCt = 0;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + UPDATE_SQL);
			}
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(UPDATE_SQL);
			Iterator<Integer> i = id.iterator();
			while (i.hasNext()) {
				int thisId = i.next();
				String charField = "f";
				int x = thisId/3;
				x = thisId - x*3;
				if (x > 1) {
					charField = "t";
				}
				ps.setString(1, charField);
				ps.setInt(2, RANDOM.nextInt(999999999));
				ps.setDate(3, new java.sql.Date(new Date().getTime() - RANDOM.nextInt(999999999)));
				ps.setTimestamp(4, new Timestamp(new Date().getTime() + RANDOM.nextInt(999999999)));
				ps.setString(5, SAMPLE_TEXT.substring(0, RANDOM.nextInt(499)));
				ps.setString(6, SAMPLE_TEXT.substring(0, RANDOM.nextInt(16000)));
				ps.setInt(7, thisId);
				updateCt += ps.executeUpdate();
			}
			if (log.isDebugEnabled()) {
				log.debug(updateCt + " records updated.");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to update the database: " + e.getMessage(), e);
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
		response += markEnd("update");
		response += " </update>\n";
		return response;
	}

	private String doDelete(HttpServletRequest req) {
		@SuppressWarnings("unchecked")
		List<Integer> id = (List<Integer>) req.getAttribute(ID_LIST);
		if (log.isDebugEnabled()) {
			log.debug("Deleting " + id.size() + " records in the database table.");
		}
		String response = " <delete>\n";
		response += markStart("delete");
		Connection conn = null;
		PreparedStatement ps = null;
		int deleteCt = 0;
		try {
			if (log.isDebugEnabled()) {
				log.debug("Using the following SQL: " + DELETE_SQL);
			}
			conn = dataSource.getConnection();
			ps = conn.prepareStatement(DELETE_SQL);
			Iterator<Integer> i = id.iterator();
			while (i.hasNext()) {
				ps.setInt(1, i.next());
				deleteCt += ps.executeUpdate();
			}
			if (log.isDebugEnabled()) {
				log.debug(deleteCt + " records deleted.");
			}
		} catch (SQLException e) {
			log.error("Exception encountered attempting to delete records from the database: " + e.getMessage(), e);
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
		response += markEnd("delete");
		response += " </delete>\n";
		return response;
	}

	private String doEnd() {
		String response = " <end>" + getTime() + "</end>\n";
		response += "</response>\n";
		if (log.isDebugEnabled()) {
			log.debug("Real-time performance monitoring completed.");
		}
		return response;
	}

	private String markStart(String qualifier) {
		return "  <" + qualifier + "Start>" + getTime() + "</" + qualifier + "Start>\n";
	}

	private String markEnd(String qualifier) {
		return "  <" + qualifier + "End>" + getTime() + "</" + qualifier + "End>\n";
	}

	private String getTime() {
		Date date = new Date();
		return date.getTime() + "";
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
