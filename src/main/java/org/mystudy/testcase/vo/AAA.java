package org.mystudy.testcase.vo;

import org.apache.spark.api.java.function.Function;

public class AAA implements Function<Integer, Integer> {
	@Override
	public Integer call(Integer v1) throws Exception {
		return v1 + 1;
	}
}