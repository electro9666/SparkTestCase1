package org.mystudy.testcase;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCase3Sol1 implements Serializable {

	private TestCase3Sol1() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase3Sol1 t = new TestCase3Sol1();
		t.proc3();
	}
	private int add(int num) {
		return num + 1;
	}
	private void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(a -> add(a)); 						// 해결
		System.out.println(rdd3.collect());
	}
}
