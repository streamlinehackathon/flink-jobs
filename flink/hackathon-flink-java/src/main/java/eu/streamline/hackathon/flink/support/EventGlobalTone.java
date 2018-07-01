package eu.streamline.hackathon.flink.support;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EventGlobalTone {
	public Date date;
	public String eventCode;
	public List<Map> countries;
	
	public EventGlobalTone() {
		countries = new LinkedList<Map>();
	}

	public EventGlobalTone(String eventCode) {
		super();
		this.eventCode = eventCode;
	}

	@Override
	public String toString() {
		return "EventGlobalTone [day=" + date + ", eventCode=" + eventCode + ", countries=" + countries + "]";
	}


}
