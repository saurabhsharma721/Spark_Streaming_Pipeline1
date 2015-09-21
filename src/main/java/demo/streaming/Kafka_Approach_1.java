package demo.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import demo.streaming.config.JavaStreamingContextFactory;
import demo.streaming.dao.SaveToRepository;
import demo.streaming.mapper.MessageToTruckEventMapper;
import demo.streaming.model.TruckEvents;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.*;
import org.apache.log4j.Logger;
import scala.Tuple2;

/*
 * This class is to integrate Kafka Recieved with Spark Streaming 
 * and update the RDD's in MongoDB
 * Main class for the application
 * Author Saurabh Sharma 
 */

public class Kafka_Approach_1 {
	
	
	//Logger for Log4j
	private static Logger logger = Logger.getLogger(Kafka_Approach_1.class);
	
	
	// Duration for batches in Spark Streaming
	private static int seconds =10;
		
	/**
	 * intialize JavaPairReceiverInputDStream for Approach1
	 * @param JavaStreamingContext , zooKeeperInstance , consumerGroup , topicMap
	 */
	public static JavaPairReceiverInputDStream<String, String>  intializeKafkaInputDStream(JavaStreamingContext jssc,
			String zooKeeperInstance, String consumerGroup, Map<String,Integer> topicMap){
			return KafkaUtils.createStream(jssc,
					zooKeeperInstance,
					consumerGroup,
				    topicMap);
	}
	
	//Main method
	public static void main(String s[]){
		logger.info("Kafka Approach 1 with Duration -" +seconds);
		
		//Creating JavaStreamingContext for calling spark streaming
		
		JavaStreamingContext jssc = JavaStreamingContextFactory.
									intializeStreamingJavaContext(seconds);
		
		//Setup checkpoint directory for saving the datasets
		jssc.checkpoint(JavaStreamingContextFactory.getCheckPointDir());
		
		
		//Truckevent is topic we will check.
		Map<String, Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("truckevent", 1);
		
		/*
		 * localhost:2181 is the broker server list
		 * TruckConsumer is the consumer group
		 * 
		 */
		
		JavaPairReceiverInputDStream<String, String> kafkaStream = intializeKafkaInputDStream(jssc,
					     "localhost:2181",
					     "TruckConsumer1",
					     topicMap);
		
		//kafkaEventsTransformer class defined below below
		JavaDStream<TruckEvents> truckEvents = kafkaStream.map(new MessageToTruckEventMapper.kafkaEventsTransformer()); 
	
		truckEvents.foreachRDD(new kafkaEventsSavetoRepoistory());
		//SaveToRepository.saveToRepository(truckEvents.);
		//truckEvents.mapToPair()
		
		//truckEvents.transform(transformFunc)saveToCassandra
		jssc.start();              // Start the computation
		jssc.awaitTermination(); 
	}
	
	
	
	public static class kafkaEventsSavetoRepoistory implements 
				Function<JavaRDD<TruckEvents>, Void> {

		/**
		 * 
		 */
		private static final long serialVersionUID = -3589220592115450806L;

		@Override
		public Void call(JavaRDD<TruckEvents> v1) throws Exception {
			// TODO Auto-generated method stub
			SaveToRepository.saveToRepository(v1);
			return null;
		}
		
	}
}	


