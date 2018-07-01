package eu.streamline.hackathon.flink.support;

import java.util.ArrayList;
import java.util.Comparator;

public class DataAccumulator
{
	public ArrayList<TopicCount> list = new ArrayList<TopicCount>();
	
	public DataAccumulator()
	{
	}
	
	public DataAccumulator add(TopicCount item)
	{
		list.add(item);
		
		if(list.size() > 5)
		{
			list.sort(new Comparator<TopicCount>()
			{
				@Override
				public int compare(TopicCount item1, TopicCount item2)
				{
					if(item1.count > item2.count)
					{
						return -1;
					}
					else if(item1.count < item2.count)
					{
						return 1;
					}
					else
					{
						return 0;
					}
				}
			});
			
			list.remove(list.size() - 1);
		}
		
		return this;
	}
	
	@Override
	public String toString()
	{
		String string = "";
		
		for(TopicCount topicCount : list)
		{
			string += topicCount.toString();
		}
		
		return string;
	}
}