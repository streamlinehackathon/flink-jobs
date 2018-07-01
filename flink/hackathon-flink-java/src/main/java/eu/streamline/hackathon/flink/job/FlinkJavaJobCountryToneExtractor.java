package eu.streamline.hackathon.flink.job;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
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

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.flink.operations.GDELTInputFormat;
import eu.streamline.hackathon.flink.support.EventGlobalTone;
import eu.streamline.hackathon.flink.utils.CountryCodeEnrichter;

public class FlinkJavaJobCountryToneExtractor {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJobCountryToneExtractor.class);

	public static void main(String[] args) {

		final String REST_SERVER = "http://10.42.0.75:5984/events_";

		ParameterTool params = ParameterTool.fromArgs(args);
		final String pathToGDELT = params.get("path", "./180-days.csv");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		// Read the input file
		DataStream<GDELTEvent> source = env.readFile(new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT)
				.setParallelism(1);

		/***
		 *  Resolve media country from URL and map the input Event to a tuple containing:
		 *  - eventCode
		 *  - country
		 *  - avgTone
		 *  - '1' for counting
		 *  - date of event
		 *  */
		
		DataStreamSink<EventGlobalTone> stream = ((DataStreamSource<GDELTEvent>) source).setParallelism(1)
				.flatMap(new FlatMapFunction<GDELTEvent, Tuple5<String, String, Double, Integer, Date>>() {

					@Override
					public void flatMap(GDELTEvent value,
							Collector<Tuple5<String, String, Double, Integer, Date>> out) {

						URL url = null;
						try {
							url = new URL(value.sourceUrl);
						} catch (MalformedURLException e) {
							System.out.println("ERROR: Malformed URL " + value.sourceUrl);
						}
						if (url != null) {
							// resolve the media contrycode
							String countryCode = CountryCodeEnrichter.INSTANCE.getCountryCode(url.getHost().trim());
							if (countryCode != null)
								out.collect(new Tuple5<String, String, Double, Integer, Date>(value.eventCode,
										countryCode, value.avgTone, 1, value.day));
						}
					}
				})
				
				// Use event date as timestamp
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Tuple5<String, String, Double, Integer, Date>>(
								Time.seconds(0)) {

							@Override
							public long extractTimestamp(Tuple5<String, String, Double, Integer, Date> element) {
								return element.f4.getTime();
							}

						})
				
				// Key by Event code and Country 
				.keyBy(0, 1)
				// Window over one day
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				
				// Average the tone of the events over the keys
				.reduce(new ReduceFunction<Tuple5<String, String, Double, Integer, Date>>() {

					@Override
					public Tuple5<String, String, Double, Integer, Date> reduce(
							Tuple5<String, String, Double, Integer, Date> t1,
							Tuple5<String, String, Double, Integer, Date> t2) throws Exception {

						return new Tuple5<String, String, Double, Integer, Date>(t1.f0, t1.f1,
								(t1.f2 * t1.f3 + t2.f2 * t2.f3) / (t1.f3 + t2.f3), t1.f3 + t2.f3, t1.f4);
					}
				})
				// Key by country
				.keyBy(0)
				// Window again
				.window(TumblingEventTimeWindows.of(Time.days(1)))
				// Fold to aggregate the tone of the different countries about the single event into a unique object
				.fold(new EventGlobalTone(),
						new FoldFunction<Tuple5<String, String, Double, Integer, Date>, EventGlobalTone>() {

							@Override
							public EventGlobalTone fold(EventGlobalTone accumulator,
									Tuple5<String, String, Double, Integer, Date> value) throws Exception {
								Map m = new HashMap<String, String>();
								m.put("countryCode", value.f1);
								m.put("value", value.f2.toString());
								accumulator.countries.add(m);
								accumulator.eventCode = value.f0;
								accumulator.date = value.f4;
								return accumulator;
							}
						})
				// Serialize the object to send a JSON REST post call to CouchDB
				.addSink(new SinkFunction<EventGlobalTone>() {

					@Override
					public void invoke(EventGlobalTone value) throws Exception {
						Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd").create();
						System.out.println(gson.toJson(value));
						StringEntity entity = new StringEntity(gson.toJson(value),
								ContentType.APPLICATION_FORM_URLENCODED);

						// Try to create the DB in case it doesn't exists (not necessary in production
						// environment)
						HttpClient httpClient = HttpClients.custom().build();
						HttpPut putReq = new HttpPut(REST_SERVER + value.eventCode);
						HttpResponse putResp = httpClient.execute(putReq);
						if (putResp.getStatusLine().getStatusCode() == 201) {
							System.out.println("Created DB " + value.eventCode);
						}

						// Send the Data to the DB
						HttpPost postReq = new HttpPost(REST_SERVER + value.eventCode);
						postReq.addHeader("content-type", "application/json");
						postReq.addHeader("Accept", "application/json");
						postReq.setEntity(entity);
//						System.out.println(postReq.toString());
						HttpResponse postResp = httpClient.execute(postReq);
//						System.out.println(postResp.getStatusLine().getStatusCode());
					}
				});

		try {
			env.execute("Flink Java GDELT Analyzer");
		} catch (Exception e) {
			LOG.error("Failed to execute Flink job {}", e);
		}
	}

}
