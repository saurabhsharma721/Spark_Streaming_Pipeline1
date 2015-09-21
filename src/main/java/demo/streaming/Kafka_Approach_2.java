package demo.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;
import demo.streaming.Kafka_Approach_1.kafkaEventsSavetoRepoistory;
import demo.streaming.config.JavaStreamingContextFactory;
import demo.streaming.dao.SaveToRepository;
import demo.streaming.mapper.MessageToTruckEventMapper;
import demo.streaming.model.TruckEvents;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;
/*
 * This cass in the preferred Approach
 * 
 */


public class Kafka_Approach_2 {
	
	private static Logger logger = Logger.getLogger(Kafka_Approach_2.class);

	private static String datePattern ="yyyy-mm-dd hh:mm:ss.SSS";
	
		
	public static void main(String s[]){
		int seconds = 10;
		logger.info("Kafka Approach 2 with Duration -" +seconds);

		JavaStreamingContext jssc = JavaStreamingContextFactory.intializeStreamingJavaContext(seconds);
		jssc.checkpoint(JavaStreamingContextFactory.getCheckPointDir());
		Set<String> topicSet = new HashSet<String>();
		topicSet.add("truckevent");
		
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
				jssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topicSet
			);
				
		//kafkaEventsTransformer class defined below below
		JavaDStream<TruckEvents> truckEvents = messages.map(new MessageToTruckEventMapper.kafkaEventsTransformer()); 
			
		truckEvents.foreachRDD(new kafkaEventsSavetoRepoistory());
		jssc.start();              // Start the computation
		jssc.awaitTermination(); 
				
		
	}
	
	public static class kafkaEventsSavetoRepoistory implements 
	Function<JavaRDD<TruckEvents>, Void> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -39220592115450806L;

		@Override
		public Void call(JavaRDD<TruckEvents> v1) throws Exception {
			SaveToRepository.saveToRepository(v1);
			return null;
		}

}

}
