package eu.streamline.hackathon.flink.support;

import java.util.Date;

public class TopicCount
{
	public String eventCode  = null;
	public String eventLabel = null;
	public Integer count = 0;

	public Date timestamp = null;
	
	public TopicCount(String eventCode, String eventLabel, Integer count, Date timestamp)
	{
		this.eventCode = eventCode;
		this.eventLabel = eventLabel;
		this.count = count;
		this.timestamp = timestamp;
	}
	
	@Override
	public String toString()
	{
		return "TopicCount [eventCode=" + eventCode + ", eventLabel=" + eventLabel + ", count=" + count + ", timestamp=" + timestamp + "]";
	}
}