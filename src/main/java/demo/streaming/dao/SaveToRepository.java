package demo.streaming.dao;


import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;

import scala.Tuple2;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;

import demo.streaming.model.TruckEvents;

public class SaveToRepository {
	
	private static Configuration outputConfig;
	
	public static Configuration getOutputConfiguration(){
		if(null==outputConfig){
			outputConfig = new Configuration();
			outputConfig.set("mongo.output.uri",
	                 "mongodb://localhost:27017/truckEventsDemo.truckEvents");
		}
		return outputConfig;
		
	}
	
	public static void saveToRepository(JavaRDD<TruckEvents> truckEvents){
		Configuration outputConfig = getOutputConfiguration();
		JavaPairRDD<Object,BSONObject> jTruckBson = truckEvents.mapToPair(
				new PairFunction<TruckEvents, Object, BSONObject>() {
				/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Object, BSONObject> call(TruckEvents truckEvent) throws Exception {
					DBObject doc = BasicDBObjectBuilder.start()
						.add("EventTime", truckEvent.getEventTime())
						.add("LatitudeId", truckEvent.getLatitudeId())
						.add("LongitudeId", truckEvent.getLongitudeId())
						.add("TruckDriverId", truckEvent.getTruckDriverId())
						.add("TruckId", truckEvent.getTruckId())
						.get();
				// null key means an ObjectId will be generated on insert
					return new Tuple2<Object, BSONObject>(null, doc);
					}
				}
				);
		
		//Saving data to MongoDB
		jTruckBson.saveAsNewAPIHadoopFile(
			    "file:///this-is-completely-unused",
			    Object.class,
			    BSONObject.class,
			    MongoOutputFormat.class,
			    outputConfig
			);
	}
	
	
}
