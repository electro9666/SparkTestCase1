package org.test;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Ch2_10_WordCount {
	LogUtil lu = new LogUtil(true, 3, "D:\\workspace\\spark\\learning.spark\\src\\main\\java\\org\\test\\Ch2_10_WordCount.java");
	JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
	public <T> T s(T o){
		return lu.s(o);
	}
	
	public static void main(String... strings) {
		new Ch2_10_WordCount().proc1();
	}
	public void proc1(){
		String inputFile = "D:\\workspace\\spark\\learning.spark\\src\\main\\java\\org\\test\\Ch2_10_WordCount.java";
		JavaRDD<String> input = sc.textFile(inputFile);

		JavaRDD<String> words = input.flatMap(a -> Arrays.asList(a.split(" ")));

		JavaPairRDD<String, Integer> results = words.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		results = results.reduceByKey((a,b) -> a+b);
//		s(results.toString());
//		results.saveAsTextFile("d:\\resultzz.txt");
	}
}
