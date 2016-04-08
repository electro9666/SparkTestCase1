package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mystudy.testcase.vo.BBB;

public class TestCase7 {

	private TestCase7() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase7 t = new TestCase7();
		t.proc3();
	}

	private void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		JavaRDD<Integer> rdd3 = rdd2.map(a -> BBB.bbb(a)); 				//성공			
		System.out.println(rdd3.collect());
	}
}
