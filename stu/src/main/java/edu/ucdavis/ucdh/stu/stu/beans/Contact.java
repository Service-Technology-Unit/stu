package edu.ucdavis.ucdh.stu.stu.beans;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * <p>This entity bean defines a single user.</p>
 */
public class Contact implements Serializable {
	private static final long serialVersionUID = 1;
	private String id = null;
	private String sysId = null;
	private String employeeId = null;
	private String ucdLoginId = null;
	private String lastName = null;
	private String firstName = null;
	private String middleName = null;
	private String title = null;
	private String departmentId = null;
	private String department = null;
	private String supervisor = null;
	private String manager = null;
	private String email = null;
	private String alternateEmail = null;
	private String phoneNr = null;
	private String cellPhone = null;
	private String pager = null;
	private String pagerProvider = null;
	private String alternatePhones = null;
	private String confidential = null;
	private String location = null;
	private String locationName = null;
	private String address = null;
	private String city = null;
	private String state = null;
	private String zip = null;
	private String floor = null;
	private String room = null;
	private String station = null;
	private String latitude = null;
	private String longitude = null;
	private String altitude = null;
	private String itStaff = null;
	private String active = null;
	private Date validFrom = null;
	private Date validTo = null;
	private Date creationDate = null;
	private String createdBy = null;
	private Date lastUpdate = null;
	private String lastUpdateBy = null;
	private List<String> group = null;

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}
	/**
	 * @return the sysId
	 */
	public String getSysId() {
		return sysId;
	}
	/**
	 * @param sysId the sysId to set
	 */
	public void setSysId(String sysId) {
		this.sysId = sysId;
	}
	/**
	 * @return the employeeId
	 */
	public String getEmployeeId() {
		return employeeId;
	}
	/**
	 * @param employeeId the employeeId to set
	 */
	public void setEmployeeId(String employeeId) {
		this.employeeId = employeeId;
	}
	/**
	 * @return the ucdLoginId
	 */
	public String getUcdLoginId() {
		return ucdLoginId;
	}
	/**
	 * @param ucdLoginId the ucdLoginId to set
	 */
	public void setUcdLoginId(String ucdLoginId) {
		this.ucdLoginId = ucdLoginId;
	}
	/**
	 * @return the lastName
	 */
	public String getLastName() {
		return lastName;
	}
	/**
	 * @param lastName the lastName to set
	 */
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	/**
	 * @return the firstName
	 */
	public String getFirstName() {
		return firstName;
	}
	/**
	 * @param firstName the firstName to set
	 */
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	/**
	 * @return the middleName
	 */
	public String getMiddleName() {
		return middleName;
	}
	/**
	 * @param middleName the middleName to set
	 */
	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}
	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}
	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}
	/**
	 * @return the department
	 */
	public String getDepartment() {
		return department;
	}
	/**
	 * @param department the department to set
	 */
	public void setDepartment(String department) {
		this.department = department;
	}
	/**
	 * @return the supervisor
	 */
	public String getSupervisor() {
		return supervisor;
	}
	/**
	 * @param supervisor the supervisor to set
	 */
	public void setSupervisor(String supervisor) {
		this.supervisor = supervisor;
	}
	/**
	 * @return the manager
	 */
	public String getManager() {
		return manager;
	}
	/**
	 * @param manager the manager to set
	 */
	public void setManager(String manager) {
		this.manager = manager;
	}
	/**
	 * @return the email
	 */
	public String getEmail() {
		return email;
	}
	/**
	 * @param email the email to set
	 */
	public void setEmail(String email) {
		this.email = email;
	}
	/**
	 * @return the alternateEmail
	 */
	public String getAlternateEmail() {
		return alternateEmail;
	}
	/**
	 * @param alternateEmail the alternateEmail to set
	 */
	public void setAlternateEmail(String alternateEmail) {
		this.alternateEmail = alternateEmail;
	}
	/**
	 * @return the phoneNr
	 */
	public String getPhoneNr() {
		return phoneNr;
	}
	/**
	 * @param phoneNr the phoneNr to set
	 */
	public void setPhoneNr(String phoneNr) {
		this.phoneNr = phoneNr;
	}
	/**
	 * @return the cellPhone
	 */
	public String getCellPhone() {
		return cellPhone;
	}
	/**
	 * @param cellPhone the cellPhone to set
	 */
	public void setCellPhone(String cellPhone) {
		this.cellPhone = cellPhone;
	}
	/**
	 * @return the pager
	 */
	public String getPager() {
		return pager;
	}
	/**
	 * @param pager the pager to set
	 */
	public void setPager(String pager) {
		this.pager = pager;
	}
	/**
	 * @return the pagerProvider
	 */
	public String getPagerProvider() {
		return pagerProvider;
	}
	/**
	 * @param pagerProvider the pagerProvider to set
	 */
	public void setPagerProvider(String pagerProvider) {
		this.pagerProvider = pagerProvider;
	}
	/**
	 * @return the alternatePhones
	 */
	public String getAlternatePhones() {
		return alternatePhones;
	}
	/**
	 * @param alternatePhones the alternatePhones to set
	 */
	public void setAlternatePhones(String alternatePhones) {
		this.alternatePhones = alternatePhones;
	}
	/**
	 * @return the confidential
	 */
	public String getConfidential() {
		return confidential;
	}
	/**
	 * @param confidential the confidential to set
	 */
	public void setConfidential(String confidential) {
		this.confidential = confidential;
	}
	/**
	 * @return the location
	 */
	public String getLocation() {
		return location;
	}
	/**
	 * @param location the location to set
	 */
	public void setLocation(String location) {
		this.location = location;
	}
	/**
	 * @return the locationName
	 */
	public String getLocationName() {
		return locationName;
	}
	/**
	 * @param locationName the locationName to set
	 */
	public void setLocationName(String locationName) {
		this.locationName = locationName;
	}
	/**
	 * @return the address
	 */
	public String getAddress() {
		return address;
	}
	/**
	 * @param address the address to set
	 */
	public void setAddress(String address) {
		this.address = address;
	}
	/**
	 * @return the city
	 */
	public String getCity() {
		return city;
	}
	/**
	 * @param city the city to set
	 */
	public void setCity(String city) {
		this.city = city;
	}
	/**
	 * @return the state
	 */
	public String getState() {
		return state;
	}
	/**
	 * @param state the state to set
	 */
	public void setState(String state) {
		this.state = state;
	}
	/**
	 * @return the zip
	 */
	public String getZip() {
		return zip;
	}
	/**
	 * @param zip the zip to set
	 */
	public void setZip(String zip) {
		this.zip = zip;
	}
	/**
	 * @return the floor
	 */
	public String getFloor() {
		return floor;
	}
	/**
	 * @param floor the floor to set
	 */
	public void setFloor(String floor) {
		this.floor = floor;
	}
	/**
	 * @return the room
	 */
	public String getRoom() {
		return room;
	}
	/**
	 * @param room the room to set
	 */
	public void setRoom(String room) {
		this.room = room;
	}
	/**
	 * @return the station
	 */
	public String getStation() {
		return station;
	}
	/**
	 * @param station the station to set
	 */
	public void setStation(String station) {
		this.station = station;
	}
	/**
	 * @return the latitude
	 */
	public String getLatitude() {
		return latitude;
	}
	/**
	 * @param latitude the latitude to set
	 */
	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}
	/**
	 * @return the longitude
	 */
	public String getLongitude() {
		return longitude;
	}
	/**
	 * @param longitude the longitude to set
	 */
	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}
	/**
	 * @return the altitude
	 */
	public String getAltitude() {
		return altitude;
	}
	/**
	 * @param altitude the altitude to set
	 */
	public void setAltitude(String altitude) {
		this.altitude = altitude;
	}
	/**
	 * @return the itStaff
	 */
	public String getItStaff() {
		return itStaff;
	}
	/**
	 * @param itStaff the itStaff to set
	 */
	public void setItStaff(String itStaff) {
		this.itStaff = itStaff;
	}
	/**
	 * @return the departmentId
	 */
	public String getDepartmentId() {
		return departmentId;
	}
	/**
	 * @param departmentId the departmentId to set
	 */
	public void setDepartmentId(String departmentId) {
		this.departmentId = departmentId;
	}
	/**
	 * @return the active
	 */
	public String getActive() {
		return active;
	}
	/**
	 * @param active the active to set
	 */
	public void setActive(String active) {
		this.active = active;
	}
	/**
	 * @return the validFrom
	 */
	public Date getValidFrom() {
		return validFrom;
	}
	/**
	 * @param validFrom the validFrom to set
	 */
	public void setValidFrom(Date validFrom) {
		this.validFrom = validFrom;
	}
	/**
	 * @return the validTo
	 */
	public Date getValidTo() {
		return validTo;
	}
	/**
	 * @param validTo the validTo to set
	 */
	public void setValidTo(Date validTo) {
		this.validTo = validTo;
	}
	/**
	 * @return the creationDate
	 */
	public Date getCreationDate() {
		return creationDate;
	}
	/**
	 * @param creationDate the creationDate to set
	 */
	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}
	/**
	 * @return the createdBy
	 */
	public String getCreatedBy() {
		return createdBy;
	}
	/**
	 * @param createdBy the createdBy to set
	 */
	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}
	/**
	 * @return the lastUpdate
	 */
	public Date getLastUpdate() {
		return lastUpdate;
	}
	/**
	 * @param lastUpdate the lastUpdate to set
	 */
	public void setLastUpdate(Date lastUpdate) {
		this.lastUpdate = lastUpdate;
	}
	/**
	 * @return the lastUpdateBy
	 */
	public String getLastUpdateBy() {
		return lastUpdateBy;
	}
	/**
	 * @param lastUpdateBy the lastUpdateBy to set
	 */
	public void setLastUpdateBy(String lastUpdateBy) {
		this.lastUpdateBy = lastUpdateBy;
	}
	/**
	 * @return the group
	 */
	public List<String> getGroup() {
		return group;
	}
	/**
	 * @param group the group to set
	 */
	public void setGroup(List<String> group) {
		this.group = group;
	}
	/**
	 * @return true if the contact is a member of the group
	 */
	public boolean isMemberOf(String groupName) {
		return group.contains(groupName);
	}
}
