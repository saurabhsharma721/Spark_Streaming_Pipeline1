package demo.streaming;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import demo.streaming.config.JavaStreamingContextFactory;
import demo.streaming.dao.SaveToRepository;
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
	
	//Datepattern for Dates coming from Kafka
	private static String datePattern ="yyyy-mm-dd hh:mm:ss.SSS";
	
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
		JavaDStream<TruckEvents> truckEvents = kafkaStream.map(new kafkaEventsTransformer()); 
	
		truckEvents.foreachRDD(new kafkaEventsSavetoRepoistory());
		//SaveToRepository.saveToRepository(truckEvents.);
		//truckEvents.mapToPair()
		
		//truckEvents.transform(transformFunc)saveToCassandra
		jssc.start();              // Start the computation
		jssc.awaitTermination(); 
	}
	
	//Transform input string into TruckEvents objects for further processing
	public static class kafkaEventsTransformer implements 
						Function<Tuple2<String, String>, TruckEvents> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public TruckEvents call(Tuple2<String, String> tuple2) throws ParseException {
			String truckEvents[] =tuple2._2().split("\\|");
			logger.info("Message Recieved from Kafka: " +tuple2._2());
			TruckEvents tr = new TruckEvents();
			if(truckEvents.length>0){
				logger.info(tuple2._2());
				logger.info(truckEvents[0]);
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


