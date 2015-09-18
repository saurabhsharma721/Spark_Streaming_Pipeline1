package demo.streaming.config;

import org.apache.spark.SparkConf;

public class SparkContextFactory {
	
	private static SparkConf sparkConf;
	private SparkContextFactory(){
		
	}
	
	public static SparkConf getSparkContextInstance(){
		if(null==sparkConf){
			sparkConf = new SparkConf().setMaster("local[3]")
					.setAppName("Kafka_Streaming");
		}
		return sparkConf;
	}

}
