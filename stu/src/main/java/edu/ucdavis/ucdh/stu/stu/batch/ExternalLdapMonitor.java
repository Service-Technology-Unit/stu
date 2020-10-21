package edu.ucdavis.ucdh.stu.stu.batch;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
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
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.ucdavis.ucdh.stu.core.batch.SpringBatchJob;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobService;
import edu.ucdavis.ucdh.stu.core.utils.BatchJobServiceStatistic;
import edu.ucdavis.ucdh.stu.core.utils.HttpClientProvider;
import edu.ucdavis.ucdh.stu.snutil.beans.Event;
import edu.ucdavis.ucdh.stu.snutil.util.EventService;

/**
 * <p>Looks for password change dates and last login dates in A/D for new Externals.</p>
 */
public class ExternalLdapMonitor implements SpringBatchJob {
	private static final long DIFF_NET_JAVA_FOR_DATES = 11644473600000L + 24 * 60 * 60 * 1000;
	private static final String FETCH_URL = "/api/now/table/x_ucdhs_identity_s_identity?sysparm_query=active%3Dtrue%5Elan_required%3Dtrue%5Euser_nameISNOTEMPTY%5Eexternal_idSTARTSWITHH00%5Ea_d_account_claimedISEMPTY%5EORa_d_account_usedISEMPTY&sysparm_display_value=false&sysparm_fields=sys_id%2Cuser_name%2Ca_d_account_claimed%2Ca_d_account_used";
	private static final String UPDATE_URL = "/api/now/table/x_ucdhs_identity_s_identity/";
	private final Log log = LogFactory.getLog(getClass().getName());
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
	private String serviceNowServer = null;
	private String serviceNowUser = null;
	private String serviceNowPassword = null;
	private EventService eventService = null;
	private JSONArray users = null;
	private int recordsRead = 0;
	private int adAccountsFound = 0;
	private int recordsUpdated = 0;

	public List<BatchJobServiceStatistic> run(String[] args, int batchJobInstanceId) throws Exception {
		externalBegin();
		if (users != null && users.size() > 0) {
			for (int i=0; i<users.size(); i++) {
				external((JSONObject) users.get(i));
			}
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
		// verify serviceNowServer
		if (StringUtils.isEmpty(serviceNowServer)) {
			throw new IllegalArgumentException("Required property \"serviceNowServer\" missing or invalid.");
		} else {
			log.info("serviceNowServer = " + serviceNowServer);
		}
		// verify serviceNowUser
		if (StringUtils.isEmpty(serviceNowUser)) {
			throw new IllegalArgumentException("Required property \"serviceNowUser\" missing or invalid.");
		} else {
			log.info("serviceNowUser = " + serviceNowUser);
		}
		// verify serviceNowPassword
		if (StringUtils.isEmpty(serviceNowPassword)) {
			throw new IllegalArgumentException("Required property \"serviceNowPassword\" missing or invalid.");
		} else {
			log.info("serviceNowPassword = **********");
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

		// fetch external users
		loadExternalUsers();
	}

	private List<BatchJobServiceStatistic> externalEnd() throws Exception {
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

	@SuppressWarnings("unchecked")
	private void external(JSONObject external) throws Exception {
		recordsRead++;
		String sysId = (String) external.get("sys_id");
		String adId = (String) external.get("user_name");
		String accountClaimed = (String) external.get("a_d_account_claimed");
		if (log.isDebugEnabled()) {
			log.debug("Processing user " + adId);
		}
		Map<String,Timestamp> ldapData = getLdapData(adId);
		if (ldapData.get("ACCOUNT_USED") != null || (StringUtils.isEmpty(accountClaimed) && ldapData.get("ACCOUNT_CLAIMED") != null)) {
			String newAccountClaimed = null;
			String newAccountUsed = null;
			if (StringUtils.isEmpty(accountClaimed) && ldapData.get("ACCOUNT_CLAIMED") != null) {
				newAccountClaimed = ldapData.get("ACCOUNT_CLAIMED").toString();
			}
			if (ldapData.get("ACCOUNT_USED") != null) {
				newAccountUsed = ldapData.get("ACCOUNT_USED").toString();
			}
			String url = serviceNowServer + UPDATE_URL + sysId;
			HttpPut put = new HttpPut(url);
			put.setHeader(HttpHeaders.ACCEPT, "application/json");
			put.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
			JSONObject updateData = new JSONObject();
			if (StringUtils.isNotEmpty(newAccountClaimed)) {
				updateData.put("a_d_account_claimed", newAccountClaimed);
			}
			if (StringUtils.isNotEmpty(newAccountUsed)) {
				updateData.put("a_d_account_used", newAccountUsed);
			}
			if (log.isDebugEnabled()) {
				log.debug("JSON object to PUT: " + updateData.toJSONString());
			}
			try {
				put.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), put, null));
				put.setEntity(new StringEntity(updateData.toJSONString()));
				HttpClient client = HttpClientProvider.getClient();
				if (log.isDebugEnabled()) {
					log.debug("Putting JSON update to " + url);
				}
				HttpResponse resp = client.execute(put);
				int rc = resp.getStatusLine().getStatusCode();
				if (rc == 200) {
					recordsUpdated++;
					if (log.isDebugEnabled()) {
						log.debug("HTTP response code from put: " + rc);
						log.debug(adId + " successfully updated.");
					}
				} else {
					if (log.isDebugEnabled()) {
						log.debug("Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc);
					}
					eventService.logEvent(new Event(adId, "External update error", "Invalid HTTP Response Code returned when updating sys_id " + sysId + ": " + rc));
				}
				if (log.isDebugEnabled()) {
					String jsonRespString = "";
					HttpEntity entity = resp.getEntity();
					if (entity != null) {
						jsonRespString = EntityUtils.toString(entity);
					}
					JSONObject result = (JSONObject) JSONValue.parse(jsonRespString);
					log.debug("JSON response: " + result.toJSONString());
				}
			} catch (Exception e) {
				log.debug("Exception occured when attempting to update External with AD Account " + adId + ": " + e);
				eventService.logEvent(new Event(adId, "User update exception", "Exception occured when attempting to update External with AD Account " + adId + ": " + e, null, e));
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
			Calendar calendar = new GregorianCalendar();
			calendar.set(Calendar.MILLISECOND, (int) milliseconds);
			milliseconds = milliseconds - (calendar.get(Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET));
			ts = new Timestamp(milliseconds);
			if (log.isDebugEnabled()) {
				log.debug("Date/Time conversion: attr=" + attr + "; adTimeUnits=" + adTimeUnits + "; milliseconds=" + milliseconds + "; ts=" + ts);
			}
		}

		return ts;
	}

	/**
	 * <p>Loads the list of ServiceNow sys_ids for all IT departments.</p>
	 *
	 */
	private void loadExternalUsers() {
		try {
			String url = serviceNowServer + FETCH_URL;
			if (log.isDebugEnabled()) {
				log.debug("Fetching ServiceNow External Users using URL " + url);
			}
			HttpGet get = new HttpGet(url);
			get.addHeader(new BasicScheme(StandardCharsets.UTF_8).authenticate(new UsernamePasswordCredentials(serviceNowUser, serviceNowPassword), get, null));
			get.setHeader(HttpHeaders.ACCEPT, "application/json");
			HttpClient client = HttpClientProvider.getClient();
			HttpResponse response = client.execute(get);
			int rc = response.getStatusLine().getStatusCode();
			if (log.isDebugEnabled()) {
				log.debug("HTTP response code: " + rc);
			}
			String resp = "";
			HttpEntity entity = response.getEntity();
			if (entity != null) {
				resp = EntityUtils.toString(entity);
			}
			if (rc != 200) {
				if (log.isDebugEnabled()) {
					log.debug("Invalid HTTP Response Code returned when fetching ServiceNow External Users: " + rc);
				}
			}
			JSONObject json = (JSONObject) JSONValue.parse(resp);
			if (json != null) {
				users = (JSONArray) json.get("result");
			}
		} catch (Exception e) {
			log.error("Exception encountered when fetching ServiceNow External Users: " + e, e);
			eventService.logEvent(new Event("External User List", "External User fetch exception", "Exception encountered when fetching ServiceNow External Users: " + e, null, e));
		}
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

	public void setServiceNowServer(String serviceNowServer) {
		this.serviceNowServer = serviceNowServer;
	}

	public void setServiceNowUser(String serviceNowUser) {
		this.serviceNowUser = serviceNowUser;
	}

	public void setServiceNowPassword(String serviceNowPassword) {
		this.serviceNowPassword = serviceNowPassword;
	}

	public void setEventService(EventService eventService) {
		this.eventService = eventService;
	}
}
