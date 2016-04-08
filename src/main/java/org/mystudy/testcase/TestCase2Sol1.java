package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCase2Sol1 {
	private int num1 = 4;
	JavaSparkContext sc = null;

	private TestCase2Sol1() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
		sc = new JavaSparkContext("local[2]", "First Spark App");
	}

	public static void main(String... strings) {
		TestCase2Sol1 t = new TestCase2Sol1();
		t.proc3();
	}

	private void proc3() {
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		int num1 = this.num1;										// 해결
		JavaRDD<Integer> rdd3 = rdd2.map(a -> a + num1); 			// 해결
		System.out.println(rdd3.collect());
	}
}
