package demo.streaming.mapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import demo.streaming.Kafka_Approach_1;
import demo.streaming.model.TruckEvents;

public class MessageToTruckEventMapper {
	
	private static Logger logger = Logger.getLogger(MessageToTruckEventMapper.class);

	//Datepattern for Dates coming from Kafka
	private static String datePattern ="yyyy-mm-dd hh:mm:ss.SSS";
		
	
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

}
