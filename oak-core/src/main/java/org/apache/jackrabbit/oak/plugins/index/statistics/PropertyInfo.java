package org.apache.jackrabbit.oak.plugins.index.statistics;

public class PropertyInfo {

	private final String name;
	private long count;

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

	@Override
	public String toString() {
		return name + "=" + count;
	}
}
