package com.ibm.streamsx.topology.test.beam;

import com.ibm.streamsx.topology.function.Supplier;

public class CharacterSupplier implements Supplier<Character> {

	private long cnt;

	public CharacterSupplier() {
		cnt = 0;
	}

	@Override
	public Character get() {
		char ret = (char)('a' + cnt);
		cnt = ++cnt % 26;
		return ret;
	}
}