import java.util.Arrays;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class SparkCompare {
	
	public static class Region implements Serializable{
		private String name;
		private double popularity;
		private double IDF;
		
		public Region(String name, double popularity, double idf) {
			this.name = name;
			this.popularity = popularity;
			this.IDF = idf;
		}
	}
	
	public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("SongComparison").setMaster("local"));
        JavaRDD<String> songFile = sc.textFile(args[0]);
        /*JavaPairRDD<String, Integer> songWords = songFile.flatMap(s -> Arrays.asList(s.split(" ")).iterator())
        	    .mapToPair(word -> new Tuple2<>(word, 1))
        	    .reduceByKey((a, b) -> a + b);
        songWords.collect();
        JavaRDD<String> regionFile = sc.textFile("Test/regions");
        JavaPairRDD<String, Region> regionWords = regionFile.mapToPair(f -> {
        	String[] split = f.split("\t");
        	return new Tuple2<String,Region>(split[1], new Region(split[0],Double.parseDouble(split[2]),Double.parseDouble(split[3])));
        });
        regionWords.collect();
        
        JavaPairRDD<String, Double> joinWords = songWords.join(regionWords).values().mapToPair(s -> {
        	double val = s._1*s._2.IDF - s._2.popularity;
        	val = val*val;
            return new Tuple2<String,Double>(s._2.name,val);
         }).reduceByKey((a,b)-> a+b);
        sc.parallelize(joinWords.collect()).saveAsTextFile("resuls");*/
        sc.stop();
        return;
    }
}
