package org.mystudy;

import java.io.Serializable;

public class AvgCount implements Serializable {
	private static final long serialVersionUID = 1232323L;

	public int total;
	public int num;

	public AvgCount(int total, int num) {
		this.total = total;
	}

	public double avg() {
		return this.total / (double) this.num;
	}

}
