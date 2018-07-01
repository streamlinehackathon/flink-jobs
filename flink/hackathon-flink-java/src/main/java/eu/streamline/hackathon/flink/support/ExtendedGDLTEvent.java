package eu.streamline.hackathon.flink.support;

import eu.streamline.hackathon.common.data.GDELTEvent;

public class ExtendedGDLTEvent extends GDELTEvent {
	public String mediaCountry;

	public ExtendedGDLTEvent(GDELTEvent event, String mediaCountry) {
		super();

		globalEventID = event.globalEventID;
		day = event.day;
		month = event.month;
		year = event.year;
		fracDate = event.fracDate;
		isRoot = event.isRoot;
		eventCode = event.eventCode;
		eventBaseCode = event.eventBaseCode;
		eventRootCode = event.eventRootCode;
		quadClass = event.quadClass;
		goldstein = event.goldstein;
		numMentions = event.numMentions;
		numSources = event.numSources;
		numArticles = event.numArticles;
		avgTone = event.avgTone;
		dateAdded = event.dateAdded;
		sourceUrl = event.sourceUrl;
		actor1Code_code = event.actor1Code_code;
		actor2Code_code = event.actor2Code_code;
		actor1Code_name = event.actor1Code_name;
		actor2Code_name = event.actor2Code_name;
		actor1Code_countryCode = event.actor1Code_countryCode;
		actor2Code_countryCode = event.actor2Code_countryCode;
		actor1Code_knownGroupCode = event.actor1Code_knownGroupCode;
		actor2Code_knownGroupCode = event.actor2Code_knownGroupCode;
		actor1Code_ethnicCode = event.actor1Code_ethnicCode;
		actor2Code_ethnicCode = event.actor2Code_ethnicCode;
		actor1Code_religion1Code = event.actor1Code_religion1Code;
		actor2Code_religion1Code = event.actor2Code_religion1Code;
		actor1Code_religion2Code = event.actor1Code_religion2Code;
		actor2Code_religion2Code = event.actor2Code_religion2Code;
		actor1Code_type1Code = event.actor1Code_type1Code;
		actor2Code_type1Code = event.actor2Code_type1Code;
		actor1Code_type2Code = event.actor1Code_type2Code;
		actor2Code_type2Code = event.actor2Code_type2Code;
		actor1Code_type3Code = event.actor1Code_type3Code;
		actor2Code_type3Code = event.actor2Code_type3Code;
		actor1Geo_type = event.actor1Geo_type;
		actor2Geo_type = event.actor2Geo_type;
		eventGeo_type = event.eventGeo_type;
		actor1Geo_name = event.actor1Geo_name;
		actor2Geo_name = event.actor2Geo_name;
		eventGeo_name = event.eventGeo_name;
		actor1Geo_countryCode = event.actor1Geo_countryCode;
		actor2Geo_countryCode = event.actor2Geo_countryCode;
		eventGeo_countryCode = event.eventGeo_countryCode;
		actor1Geo_adm1Code = event.actor1Geo_adm1Code;
		actor2Geo_adm1Code = event.actor2Geo_adm1Code;
		eventGeo_adm1Code = event.eventGeo_adm1Code;
		actor1Geo_lat = event.actor1Geo_lat;
		actor2Geo_lat = event.actor2Geo_lat;
		eventGeo_lat = event.eventGeo_lat;
		actor1Geo_long = event.actor1Geo_long;
		actor2Geo_long = event.actor2Geo_long;
		eventGeo_long = event.eventGeo_long;
		actor1Geo_featureId = event.actor1Geo_featureId;
		actor2Geo_featureId = event.actor2Geo_featureId;
		eventGeo_featureId = event.eventGeo_featureId;
		this.mediaCountry = mediaCountry;
	}

	@Override
	public String toString() {
		return "GDELTEvent{" + "globalEventID=" + globalEventID + ", day=" + day + ", month=" + month + ", year=" + year
				+ ", fracDate=" + fracDate + ", isRoot=" + isRoot + ", eventCode='" + eventCode + '\''
				+ ", eventBaseCode='" + eventBaseCode + '\'' + ", eventRootCode='" + eventRootCode + '\''
				+ ", quadClass=" + quadClass + ", goldstein=" + goldstein + ", numMentions=" + numMentions
				+ ", numSources=" + numSources + ", numArticles=" + numArticles + ", avgTone=" + avgTone
				+ ", dateAdded=" + dateAdded + ", sourceUrl='" + sourceUrl + '\'' + ", actor1Code_code='"
				+ actor1Code_code + '\'' + ", actor2Code_code='" + actor2Code_code + '\'' + ", actor1Code_name='"
				+ actor1Code_name + '\'' + ", actor2Code_name='" + actor2Code_name + '\'' + ", actor1Code_countryCode='"
				+ actor1Code_countryCode + '\'' + ", actor2Code_countryCode='" + actor2Code_countryCode + '\''
				+ ", actor1Code_knownGroupCode='" + actor1Code_knownGroupCode + '\'' + ", actor2Code_knownGroupCode='"
				+ actor2Code_knownGroupCode + '\'' + ", actor1Code_ethnicCode='" + actor1Code_ethnicCode + '\''
				+ ", actor2Code_ethnicCode='" + actor2Code_ethnicCode + '\'' + ", actor1Code_religion1Code='"
				+ actor1Code_religion1Code + '\'' + ", actor2Code_religion1Code='" + actor2Code_religion1Code + '\''
				+ ", actor1Code_religion2Code='" + actor1Code_religion2Code + '\'' + ", actor2Code_religion2Code='"
				+ actor2Code_religion2Code + '\'' + ", actor1Code_type1Code='" + actor1Code_type1Code + '\''
				+ ", actor2Code_type1Code='" + actor2Code_type1Code + '\'' + ", actor1Code_type2Code='"
				+ actor1Code_type2Code + '\'' + ", actor2Code_type2Code='" + actor2Code_type2Code + '\''
				+ ", actor1Code_type3Code='" + actor1Code_type3Code + '\'' + ", actor2Code_type3Code='"
				+ actor2Code_type3Code + '\'' + ", actor1Geo_type=" + actor1Geo_type + ", actor2Geo_type="
				+ actor2Geo_type + ", eventGeo_type=" + eventGeo_type + ", actor1Geo_name='" + actor1Geo_name + '\''
				+ ", actor2Geo_name='" + actor2Geo_name + '\'' + ", eventGeo_name='" + eventGeo_name + '\''
				+ ", actor1Geo_countryCode='" + actor1Geo_countryCode + '\'' + ", actor2Geo_countryCode='"
				+ actor2Geo_countryCode + '\'' + ", eventGeo_countryCode='" + eventGeo_countryCode + '\''
				+ ", actor1Geo_adm1Code='" + actor1Geo_adm1Code + '\'' + ", actor2Geo_adm1Code='" + actor2Geo_adm1Code
				+ '\'' + ", eventGeo_adm1Code='" + eventGeo_adm1Code + '\'' + ", actor1Geo_lat=" + actor1Geo_lat
				+ ", actor2Geo_lat=" + actor2Geo_lat + ", eventGeo_lat=" + eventGeo_lat + ", actor1Geo_long="
				+ actor1Geo_long + ", actor2Geo_long=" + actor2Geo_long + ", eventGeo_long=" + eventGeo_long
				+ ", actor1Geo_featureId=" + actor1Geo_featureId + ", media_country=" + mediaCountry + '}';
	}

}
