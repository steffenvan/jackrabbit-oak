package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.Objects;

public class PropertyInfo {

	private final String name;
	private long count;
	private long hash64;

	PropertyInfo(String name, long count, long hash64) {
		this.name = name;
		this.count = count;
		this.hash64 = hash64;
	}

	PropertyInfo(String name, long count) {
		this.name = name;
		this.count = count;
	}

	public long getCount() {
		return count;
	}

	public String getName() {
		return name;
	}

	public long getHash64() {
		return hash64;
	}

	@Override
	public String toString() {
		return name + "->" + count;
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, hash64);
	}

}
