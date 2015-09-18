package demo.streaming.config;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class JavaStreamingContextFactory {
	
	private static String checkPointDir="/home/cloudera/checkdir";
	
	
	public static String getCheckPointDir() {
		return checkPointDir;
	}

	public static JavaStreamingContext intializeStreamingJavaContext(Integer durationSeconds){
		SparkConf conf =  SparkContextFactory.getSparkContextInstance();
		JavaStreamingContext jssc = new JavaStreamingContext(SparkContextFactory.getSparkContextInstance(),
				Durations.seconds(durationSeconds));
		return jssc;
	}

}
