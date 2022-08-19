package org.apache.jackrabbit.oak.plugins.index.statistics;

public class PropertyInfo {

	String name;
	long count;

	PropertyInfo(String name, long count) {
		this.name = name;
		this.count = count;
	}

	public Long getCount() {
		return count;
	}

	@Override
	public String toString() {
		return name + "->" + count;
	}

}
