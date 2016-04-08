package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class TestCase4Sol1 {

	private TestCase4Sol1() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase4Sol1 t = new TestCase4Sol1();
		System.out.println("t:"+t);
		t.proc3();
	}
	class AAA implements Function<Integer, Integer> {
		@Override
		public Integer call(Integer v1) throws Exception {
			return v1 + 1;
		}
	}
	private void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(new AAA());						//Exception
		System.out.println(rdd3.collect());
	}
}
