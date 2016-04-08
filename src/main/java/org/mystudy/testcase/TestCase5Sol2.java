package org.mystudy.testcase;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class TestCase5Sol2 implements Serializable {

	private TestCase5Sol2() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase5Sol2 t = new TestCase5Sol2();
		t.proc3();
	}

	public void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		Function<Integer, Integer> f = new Function<Integer, Integer>() {
			@Override
			public Integer call(Integer v1) throws Exception {
				return v1 + 1;
			}
		};
		JavaRDD<Integer> rdd3 = rdd2.map(f); 			// 해결
		System.out.println(rdd3.collect());
	}
}
