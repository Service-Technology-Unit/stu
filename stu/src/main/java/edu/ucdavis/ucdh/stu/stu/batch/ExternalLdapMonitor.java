package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attribute;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;

/**
 * <p>Looks for password change dates and last login dates in A/D for new Externals.</p>
 */
public class ExternalLdapMonitor implements SpringBatchJob {
	private static final long DIFF_NET_JAVA_FOR_DATES = 11644473600000L + 24 * 60 * 60 * 1000;
	private static final String SEARCH_SQL = "SELECT AD_ID, ACCOUNT_CLAIMED, ACCOUNT_USED, SYSMODTIME, SYSMODCOUNT, SYSMODUSER FROM UCDEXTERNALM1 WHERE ACTIVE_IND='t' AND REQUIRE_LAN='t' AND AD_ID IS NOT NULL AND EXT_RESTID LIKE 'H0%' AND (ACCOUNT_CLAIMED IS NULL OR ACCOUNT_USED IS NULL)";
	private final Log log = LogFactory.getLog(getClass().getName());
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private Hashtable<String,String> env = null;
	private String contextFactory = null;
	DirContext ctx = null;
	private SearchControls ctls = null;
	private String[] searchOption = null;
	private String providerUrl = null;
	private String securityAuth = null;
	private String securityPrin = null;
	private String securityCred = null;
	private String standardSearch = null;
	private String smDriver = null;
	private String smURL = null;
	private String smUser = null;
	private String smPassword = null;
	private int recordsRead = 0;
	private int adAccountsFound = 0;
	private int recordsUpdated = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		externalBegin();
		while (rs.next()) {
			external();
		}
		return externalEnd();
	}

	private void externalBegin() throws Exception {
		log.info("ExternalLdapMonitor starting ...");
		log.info(" ");

		log.info("Validating run time properties ...");
		log.info(" ");

		// verify contextFactory
		if (StringUtils.isEmpty(contextFactory)) {
			throw new IllegalArgumentException("Required property \"contextFactory\" missing or invalid.");
		} else {
			log.info("contextFactory = " + contextFactory);
		}
		// verify providerUrl
		if (StringUtils.isEmpty(providerUrl)) {
			throw new IllegalArgumentException("Required property \"providerUrl\" missing or invalid.");
		} else {
			log.info("providerUrl = " + providerUrl);
		}
		// verify securityAuth
		if (StringUtils.isEmpty(securityAuth)) {
			throw new IllegalArgumentException("Required property \"securityAuth\" missing or invalid.");
		} else {
			log.info("securityAuth = " + securityAuth);
		}
		// verify securityPrins
		if (StringUtils.isEmpty(securityPrin)) {
			throw new IllegalArgumentException("Required property \"notesUser\" missing or invalid.");
		} else {
			log.info("securityPrin = " + securityPrin);
		}
		// verify securityCred
		if (StringUtils.isEmpty(securityCred)) {
			throw new IllegalArgumentException("Required property \"securityCred\" missing or invalid.");
		} else {
			log.info("securityCred = **********");
		}
		// verify Standard Search
		if (StringUtils.isEmpty(standardSearch)) {
			throw new IllegalArgumentException("Required property \"standardSearch\" missing or invalid.");
		} else {
			searchOption = standardSearch.split("\n");
			for (int i=0; i<searchOption.length; i++) {
				log.info("searchOption[" + i + "] = " + searchOption[i]);
			}
		}		
		// verify smDriver
		if (StringUtils.isEmpty(smDriver)) {
			throw new IllegalArgumentException("Required property \"smDriver\" missing or invalid.");
		} else {
			log.info("smDriver = " + smDriver);
		}
		// verify smUser
		if (StringUtils.isEmpty(smUser)) {
			throw new IllegalArgumentException("Required property \"smUser\" missing or invalid.");
		} else {
			log.info("smUser = " + smUser);
		}
		// verify smPassword
		if (StringUtils.isEmpty(smPassword)) {
			throw new IllegalArgumentException("Required property \"smPassword\" missing or invalid.");
		} else {
			log.info("smPassword = ********");
		}
		// verify smURL
		if (StringUtils.isEmpty(smURL)) {
			throw new IllegalArgumentException("Required property \"smURL\" missing or invalid.");
		} else {
			log.info("smURL = " + smURL);
		}

		log.info(" ");
		log.info("Run time properties validated.");
		log.info(" ");

		// establish LDAP search environment
		env = new Hashtable<String,String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY, contextFactory);
		env.put(Context.PROVIDER_URL, providerUrl);					   
		env.put(Context.SECURITY_AUTHENTICATION, securityAuth);					   
		env.put(Context.SECURITY_PRINCIPAL, securityPrin);	
		env.put(Context.SECURITY_CREDENTIALS, securityCred);
		ctx = new InitialDirContext(env);
		ctls = new SearchControls();
		ctls.setSearchScope(SearchControls.SUBTREE_SCOPE);

		// connect to ServiceManager database
		Class.forName(smDriver);
		conn = DriverManager.getConnection(smURL, smUser, smPassword);
		stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		rs = stmt.executeQuery(SEARCH_SQL);
	}

	private List<BatchJobServiceStatistic> externalEnd() throws Exception {
		// close database connection
		stmt.close();
		rs.close();
		conn.close();

		// prepare job statistics
		List<BatchJobServiceStatistic> stats = new ArrayList<BatchJobServiceStatistic>();
		if (recordsRead > 0) {
			stats.add(new BatchJobServiceStatistic("External master records read", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsRead + "")));
		}
		if (adAccountsFound > 0) {
			stats.add(new BatchJobServiceStatistic("Active Directory records found", BatchJobService.FORMAT_INTEGER, new BigInteger(adAccountsFound + "")));
		}
		if (recordsUpdated > 0) {
			stats.add(new BatchJobServiceStatistic("External master records updated", BatchJobService.FORMAT_INTEGER, new BigInteger(recordsUpdated + "")));
		}

		// end job
		log.info(" ");
		log.info("ExternalLdapMonitor complete.");

		return stats;
	}

	private void external() throws Exception {
		recordsRead++;
		String adId = rs.getString("AD_ID");
		int sysmodcount = rs.getInt("SYSMODCOUNT");
		Timestamp accountClaimed = rs.getTimestamp("ACCOUNT_CLAIMED");
		if (log.isDebugEnabled()) {
			log.debug("Processing user " + adId);
		}
		Map<String,Timestamp> ldapData = getLdapData(adId);
		if (ldapData.get("ACCOUNT_USED") != null || (accountClaimed == null && ldapData.get("ACCOUNT_CLAIMED") != null)) {
			if (accountClaimed == null && ldapData.get("ACCOUNT_CLAIMED") != null) {
				rs.updateTimestamp("ACCOUNT_CLAIMED", ldapData.get("ACCOUNT_CLAIMED"));
			}
			if (ldapData.get("ACCOUNT_USED") != null) {
				rs.updateTimestamp("ACCOUNT_USED", ldapData.get("ACCOUNT_USED"));
			}
			rs.updateString("SYSMODUSER", "extldap");
			rs.updateTimestamp("SYSMODTIME", new Timestamp(new Date().getTime()));
			rs.updateInt("SYSMODCOUNT", sysmodcount++);
			rs.updateRow();
			recordsUpdated++;
			if (log.isDebugEnabled()) {
				log.debug(adId + " successfully updated.");
			}
		}
	}

	private Map<String,Timestamp> getLdapData(String adId) throws Exception {
		Map<String,Timestamp> ldapData = new HashMap<String,Timestamp>();

		for (int i=0; i<searchOption.length; i++) {
			NamingEnumeration<SearchResult> e = ctx.search(searchOption[i], "sAMAccountName=" + adId, ctls);
			if (e.hasMoreElements()) {
				SearchResult sr = e.nextElement();
				adAccountsFound++;
				if (log.isDebugEnabled()) {
					log.debug("A/D record found for " + adId);
				}
				if (sr.getAttributes().get("pwdLastSet") != null) {
					if (log.isDebugEnabled()) {
						log.debug("pwdLastSet attribute present for " + adId);
					}
					ldapData.put("ACCOUNT_CLAIMED", getTimeStamp(sr.getAttributes().get("pwdLastSet")));
				}
				if (sr.getAttributes().get("lastLogonTimestamp") != null) {
					if (log.isDebugEnabled()) {
						log.debug("lastLogonTimestamp attribute present for " + adId);
					}
					ldapData.put("ACCOUNT_USED", getTimeStamp(sr.getAttributes().get("lastLogonTimestamp")));
				}
				i = searchOption.length;
			}
		}

		return ldapData;
	}

	private Timestamp getTimeStamp(Attribute attr) throws Exception {
		Timestamp ts = null;

		long adTimeUnits = Long.parseLong((String) attr.get());
		if (adTimeUnits > 0) {
			long milliseconds = (adTimeUnits / 10000) - DIFF_NET_JAVA_FOR_DATES;
			ts = new Timestamp(milliseconds);
			if (log.isDebugEnabled()) {
				log.debug("Date/Time conversion: attr=" + attr + "; adTimeUnits=" + adTimeUnits + "; milliseconds=" + milliseconds + "; ts=" + ts);
			}
		}

		return ts;
	}

	/**
	 * @param contextFactory the contextFactory to set
	 */
	public void setContextFactory(String contextFactory) {
		this.contextFactory = contextFactory;
	}

	/**
	 * @param providerUrl the providerUrl to set
	 */
	public void setProviderUrl(String providerUrl) {
		this.providerUrl = providerUrl;
	}

	/**
	 * @param securityAuth the securityAuth to set
	 */
	public void setSecurityAuth(String securityAuth) {
		this.securityAuth = securityAuth;
	}

	/**
	 * @param securityPrin the securityPrin to set
	 */
	public void setSecurityPrin(String securityPrin) {
		this.securityPrin = securityPrin;
	}

	/**
	 * @param securityCred the securityCred to set
	 */
	public void setSecurityCred(String securityCred) {
		this.securityCred = securityCred;
	}

	/**
	 * @param standardSearch the standardSearch to set
	 */
	public void setStandardSearch(String standardSearch) {
		this.standardSearch = standardSearch;
	}

	/**
	 * @param smDriver the smDriver to set
	 */
	public void setSmDriver(String smDriver) {
		this.smDriver = smDriver;
	}

	/**
	 * @param smURL the smURL to set
	 */
	public void setSmURL(String smURL) {
		this.smURL = smURL;
	}

	/**
	 * @param smUser the smUser to set
	 */
	public void setSmUser(String smUser) {
		this.smUser = smUser;
	}

	/**
	 * @param smPassword the smPassword to set
	 */
	public void setSmPassword(String smPassword) {
		this.smPassword = smPassword;
	}
}
