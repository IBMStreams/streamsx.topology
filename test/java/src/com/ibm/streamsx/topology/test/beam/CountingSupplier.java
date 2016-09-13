package com.ibm.streamsx.topology.test.beam;

import com.ibm.streamsx.topology.function.Supplier;

public class CountingSupplier implements Supplier<Long> {

	private long cnt;

	public CountingSupplier() {
		cnt = 0;
	}

	@Override
	public Long get() {
		return cnt ++;
	}
}