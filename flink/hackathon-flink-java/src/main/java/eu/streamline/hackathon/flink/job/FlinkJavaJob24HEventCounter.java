package eu.streamline.hackathon.flink.job;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import akka.japi.tuple.Tuple3;
import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.flink.operations.GDELTInputFormat;
import eu.streamline.hackathon.flink.support.DataAccumulator;
import eu.streamline.hackathon.flink.support.TopicCount;
import eu.streamline.hackathon.flink.support.TrendingTopics;
import eu.streamline.hackathon.flink.utils.CAMEO_CODES;

public class FlinkJavaJob24HEventCounter
{
	private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJobCountryToneExtractor.class);
	
	private static HashMap<Long, ArrayList<TopicCount>> trendingTopicsPerDay = new HashMap<Long, ArrayList<TopicCount>>();

	public static void main(String[] args)
	{
		ParameterTool params = ParameterTool.fromArgs(args);
		final String REST_SERVER = "http://10.42.0.75:5984/trending_topics";
		final String pathToGDELT = params.get("path", "./180-days.csv");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<GDELTEvent> source = env.readFile(new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT).setParallelism(1);

		source
		//Project the attributes to EventCode, 1, DateAdded
		.map(new MapFunction<GDELTEvent, Tuple3<String, Integer, Date>>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple3<String, Integer, Date> map(GDELTEvent value) throws Exception
			{
				return new Tuple3<String, Integer, Date>(value.eventCode, 1, value.dateAdded);
			}
		})
		//Set DateAdded as timestamp
		.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, Integer, Date>>(Time.seconds(0))
		{
			private static final long serialVersionUID = 1L;

			@Override
			public long extractTimestamp(Tuple3<String, Integer, Date> element)
			{
				return element.t3().getTime();
			}
		})
		//Key by EventCode
		.keyBy(new KeySelector<Tuple3<String, Integer, Date>, String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String getKey(Tuple3<String, Integer, Date> value) throws Exception
			{
				return value.t1();
			}
		})
		//Define 1 Day tumbling time window
		.window(TumblingEventTimeWindows.of(Time.days(1)))
		//Aggregate by summing up all 1s (counting) for each EventCode and replace DateAdded by starting date of time window
		.aggregate(new AggregateFunction<Tuple3<String, Integer, Date>, IntCounter, Integer>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public IntCounter createAccumulator()
			{
				return new IntCounter();
			}

			@Override
			public void add(Tuple3<String, Integer, Date> value, IntCounter accumulator)
			{
				accumulator.add(value.t2());
			}

			@Override
			public Integer getResult(IntCounter accumulator)
			{
				return accumulator.getLocalValuePrimitive();
			}

			@Override
			public IntCounter merge(IntCounter a, IntCounter b)
			{
				return new IntCounter(a.getLocalValuePrimitive() + b.getLocalValuePrimitive());
			}
		}, new WindowFunction<Integer, Tuple3<String, Integer, Date>, String, TimeWindow>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public void apply(String key, TimeWindow window, java.lang.Iterable<Integer> input, Collector<Tuple3<String, Integer, Date>> output) throws Exception
			{
				output.collect(new Tuple3<String, Integer, Date>(key, input.iterator().next(), new Date(window.getStart())));
			}
		})
		//Key by starting time of time window (its the same for alle tuples in a window)
		.keyBy(new KeySelector<Tuple3<String, Integer, Date>, Long>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Long getKey(Tuple3<String, Integer, Date> value) throws Exception
			{
				return value.t3().getTime();
			}
		})
		//Define a time window of 1 day again
		.window(TumblingEventTimeWindows.of(Time.days(1)))
		//Fold all tupels of a window to a single Accummulator Object (Only the 5 highest counts of Events Codes are kept)
		.fold(new DataAccumulator(), new FoldFunction<Tuple3<String,Integer,Date>, DataAccumulator>()
		{
			@Override
			public DataAccumulator fold(DataAccumulator accumulator, Tuple3<String, Integer, Date> value) throws Exception
			{
				return accumulator.add(new TopicCount(value.t1(), value.t1(), value.t2(), value.t3()));
			}
		})
		//Sink the data by transforming to json and storing into couchDB
		.addSink(new SinkFunction<DataAccumulator>() {
			
			@Override
			public void invoke(DataAccumulator value) throws Exception {
				
				TrendingTopics topics = new TrendingTopics();
				
				for (TopicCount topic : value.list) {
					topics.date = topic.timestamp;
					Map m = new HashMap<String, String>();
					m.put("value", topic.eventCode + " - " + CAMEO_CODES.getTileFromId(topic.eventCode));
					m.put("score", topic.count);
					topics.topics.add(m);
				}
				
				Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
				System.out.println(gson.toJson(topics));
				StringEntity entity = new StringEntity(gson.toJson(topics),
						ContentType.APPLICATION_FORM_URLENCODED);
				
				// Create the DB
				HttpClient httpClient = HttpClients.custom().build();
				HttpPut putReq = new HttpPut(REST_SERVER);
				HttpResponse putResp = httpClient.execute(putReq);
				if (putResp.getStatusLine().getStatusCode() != 201 && putResp.getStatusLine().getStatusCode() != 412) {
					System.out.println("Error creating DB ");
				}
				
				// Post the trending topics
				HttpPost postReq = new HttpPost(REST_SERVER);
				postReq.addHeader("content-type", "application/json");
				postReq.addHeader("Accept","application/json");
				postReq.setEntity(entity);
//				System.out.println(postReq.toString());
				HttpResponse postResp = httpClient.execute(postReq);
				System.out.println(postResp.getStatusLine().getStatusCode());
			}});
		try
		{
			env.execute("Flink Java GDELT Analyzer");
		}
		catch (Exception e)
		{
			LOG.error("Failed to execute Flink job {}", e);
		}
	}
}