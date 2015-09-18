package demo.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;
import demo.streaming.config.JavaStreamingContextFactory;
import demo.streaming.model.TruckEvents;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
/*
 * This class is under construction. Not tested yet
 * 
 */


public class Kafka_Approach_2 {
	
	private static Logger logger = Logger.getLogger(Kafka_Approach_2.class);

	private static String datePattern ="yyyy-mm-dd hh:mm:ss.SSS";
	
	public static JavaPairInputDStream<String, String>  intializeKafkaInputDStream(JavaStreamingContext jssc,
			Map<String,String> envParams, Set<String> topicSet){
			return KafkaUtils.createDirectStream(
					jssc,
					String.class,
					String.class,
					StringDecoder.class,
					StringDecoder.class,
					envParams,
					topicSet
				);
	}
	
	public static void main(String s[]){
		int seconds = 10;
		logger.info("Kafka Approach 2 with Duration -" +seconds);

		JavaStreamingContext jssc = JavaStreamingContextFactory.intializeStreamingJavaContext(seconds);
		jssc.checkpoint(JavaStreamingContextFactory.getCheckPointDir());
		Set<String> topicSet = new HashSet<String>();
		topicSet.add("truckevent");
		
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "localhost:2181");
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = intializeKafkaInputDStream(jssc,
				kafkaParams,
				topicSet);
		
		// Get the lines, split them into words, count the words and print
		JavaDStream<TruckEvents> truckEvents = messages.map(new Function<Tuple2<String, String>, TruckEvents>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public TruckEvents call(Tuple2<String, String> tuple2) throws ParseException {
						String truckEvents[] =tuple2._2().split("|");
						TruckEvents tr = new TruckEvents();
						if(truckEvents.length>0){
							SimpleDateFormat dt = new SimpleDateFormat(datePattern);
							tr.setEventTime(dt.parse(truckEvents[0]));
							tr.setTruckId(truckEvents[1]);
							tr.setTruckDriverId(truckEvents[2]);
							tr.setEventDetail(truckEvents[3]);
							tr.setLatitudeId(Double.parseDouble(truckEvents[4]));
							tr.setLongitudeId(Double.parseDouble(truckEvents[5]));
						}
						logger.info("Message proceesed: " +tr.toString());
						return tr;
					}
				});
			
		truckEvents.print(10);
		jssc.start();              // Start the computation
		jssc.awaitTermination(); 
				
		
	}

}
