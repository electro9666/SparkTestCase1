package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCase1 {
	JavaSparkContext sc = null;

	private TestCase1() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
		sc = new JavaSparkContext("local[2]", "First Spark App");
	}

	public static void main(String... strings) {
		TestCase1 t = new TestCase1();
		t.proc1();
		t.proc2();
	}

	private void proc1() {
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(a -> a + 1);
		System.out.println(rdd3.collect());
	}

	private void proc2() {
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		int num2 = 3;
		JavaRDD<Integer> rdd3 = rdd2.map(a -> a + num2);
		System.out.println(rdd3.collect());
	}
}
