package ebird2postgres.ebird;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

import org.apache.commons.lang3.StringUtils;

/*
0 id 
1 edit_date
2 tax_order
3 cat
4 common_name
5 scientific_name
6 subspecies_common_name
7 subspecies_scientific_name
8 obs_count 
9 breeding_code
10 breeding_cat 
11 age_sex 
12 country 
13 country_code 
14 state 
15 state_code 
16 county 
17 county_code 
18 iba_code 
19 bcr_code 
20 usfws_code 
21 atlas_block 
22 locality 
23 locality_id 
24 locality_type
25 lat
26 long
27 obs_date 
28 time_started
29 observer_id
30 checklist_id 
31 protocol_type
32 protocol_code 
33 project_code 
34 duration 
35 effort_distance_km
36 effort_area_ha
37 number_obs
38 is_complete
39 group_identifier
40 has_media
41 approved 
42 reviewed_reason 
43 trip_comments 
44 species_comments
 */
public class EBirdRecord {
	private final String id;
	private final String commonName;
	private final String scientificName;
	private final String subspeciesCommonName;
	private final String subspeciesScientificName;
	private final Integer observationCount;
	private final String countryCode;
	private final String stateCode;
	private final String countyCode;
	private final String localityId;
	private final String localityName;
	private final double latitude;
	private final double longitude;
	private final LocalDate observationDate;
	private final LocalTime startTime;
	private final String checklistId;
	private final String protocolType;
	private final String protocolCode;
	private final Integer duration;
	private final Double effortDistanceKm;
	private final Double effortAreaHa;
	private final Integer numberOfObservers;
	private final boolean isComplete;
	private final String groupId;
	private final boolean recordApproved;
	private final String approvalReason;
	
	EBirdRecord(final String[] row) throws ParseException {
		id = row[0];
		commonName = row[4];
		scientificName = row[5];
		subspeciesCommonName = row[6];
		subspeciesScientificName = row[7];
		observationCount = "X".equals(row[8])? null : Integer.parseInt(row[8]);
		countryCode = row[13];
		stateCode = row[15];
		countyCode = row[17];
		localityName = row[22];
		localityId = row[23];
		latitude = Double.parseDouble(row[25]);
		longitude = Double.parseDouble(row[26]);
		observationDate = LocalDate.parse(row[27], DateTimeFormatter.ISO_LOCAL_DATE);
		startTime = row[28] == null? null : LocalTime.parse(row[28], DateTimeFormatter.ISO_LOCAL_TIME);
		
		checklistId = row[30];
		protocolType = row[31];
		protocolCode = row[32];
		
		duration = StringUtils.isBlank(row[34])? null : Integer.parseInt(row[34]);
		effortDistanceKm = StringUtils.isBlank(row[35])? null : Double.parseDouble(row[35]);
		effortAreaHa = StringUtils.isBlank(row[36])? null : Double.parseDouble(row[36]);
		numberOfObservers = StringUtils.isBlank(row[37])? null : Integer.parseInt(row[37]);
		isComplete = "1".equals(row[38]);
		groupId = row[39];
		recordApproved = "1".equals(row[41]);
		approvalReason = row[42];
	}

	public String getId() {
		return id;
	}

	public String getCommonName() {
		return commonName;
	}

	public String getScientificName() {
		return scientificName;
	}

	public String getSubspeciesCommonName() {
		return subspeciesCommonName;
	}

	public String getSubspeciesScientificName() {
		return subspeciesScientificName;
	}

	public Integer getObservationCount() {
		return observationCount;
	}

	public String getCountryCode() {
		return countryCode;
	}

	public String getStateCode() {
		return stateCode;
	}

	public String getCountyCode() {
		return countyCode;
	}

	public String getLocalityId() {
		return localityId;
	}

	public String getLocalityName() {
		return localityName;
	}

	public double getLatitude() {
		return latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public LocalDate getObservationDate() {
		return observationDate;
	}

	public LocalTime getStartTime() {
		return startTime;
	}

	public String getChecklistId() {
		return checklistId;
	}

	public String getProtocolType() {
		return protocolType;
	}

	public String getProtocolCode() {
		return protocolCode;
	}

	public Integer getDuration() {
		return duration;
	}

	public Double getEffortDistanceKm() {
		return effortDistanceKm;
	}

	public Double getEffortAreaHa() {
		return effortAreaHa;
	}

	public Integer getNumberOfObservers() {
		return numberOfObservers;
	}

	public boolean isComplete() {
		return isComplete;
	}

	public String getGroupId() {
		return groupId;
	}

	public boolean isRecordApproved() {
		return recordApproved;
	}

	public String getApprovalReason() {
		return approvalReason;
	}

	@Override
	public String toString() {
		return "EBirdRecord [id=" + id + ", commonName=" + commonName + ", scientificName=" + scientificName
				+ ", subspeciesCommonName=" + subspeciesCommonName + ", subspeciesScientificName="
				+ subspeciesScientificName + ", observationCount=" + observationCount + ", countryCode=" + countryCode
				+ ", stateCode=" + stateCode + ", countyCode=" + countyCode + ", localityId=" + localityId
				+ ", localityName=" + localityName + ", latitude=" + latitude + ", longitude=" + longitude
				+ ", observationDate=" + observationDate + ", startTime=" + startTime + ", checklistId=" + checklistId + ", protocolType=" + protocolType
				+ ", protocolCode=" + protocolCode + ", duration=" + duration + ", effortDistanceKm=" + effortDistanceKm
				+ ", effortAreaHa=" + effortAreaHa + ", numberOfObservers=" + numberOfObservers + ", isComplete="
				+ isComplete + ", groupId=" + groupId + ", recordApproved=" + recordApproved + ", approvalReason="
				+ approvalReason + "]";
	}
}
