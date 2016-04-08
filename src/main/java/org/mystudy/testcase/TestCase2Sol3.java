package org.mystudy.testcase;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestCase2Sol3 implements Serializable {
	private int num1 = 4;

	private TestCase2Sol3() {

	}

	public static void main(String... strings) {
		TestCase2Sol3 t = new TestCase2Sol3();
		t.proc3();
	}

	private void proc3() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(a -> a + this.num1); 			// 해결
		System.out.println(rdd3.collect());
	}
}
