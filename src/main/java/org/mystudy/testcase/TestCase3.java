package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCase3 {

	private TestCase3() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase3 t = new TestCase3();
		System.out.println("t:"+t);
		t.proc3();
	}
	private int add(int num) {
		return num + 1;
	}
	private void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(a -> add(a)); 						// Exception 발생
		System.out.println(rdd3.collect());
	}
}
