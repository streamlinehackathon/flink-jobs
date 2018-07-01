package eu.streamline.hackathon.flink.support;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TrendingTopics {
	public Date date;
	public List<Map> topics;
	
	public TrendingTopics() {
		topics = new LinkedList<Map>();
	}

}
