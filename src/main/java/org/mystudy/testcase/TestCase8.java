package org.mystudy.testcase;

import java.util.Arrays;

import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.mystudy.testcase.vo.BBB;

public class TestCase8 {

	private TestCase8() {
		PropertyConfigurator.configure("D:\\workspace\\spark\\learning.spark\\src\\resources\\log4j.properties");
	}

	public static void main(String... strings) {
		TestCase8 t = new TestCase8();
		System.out.println("t:"+t);
		t.proc3();
	}

	private void proc3() {
		JavaSparkContext sc = new JavaSparkContext("local[2]", "First Spark App");
		JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
		BBB b = new BBB();
		System.out.println("b:"+b);
		JavaRDD<Integer> rdd3 = rdd2.map(a -> b.add(a)); 				//Exception			
		System.out.println(rdd3.collect());
	}
}
