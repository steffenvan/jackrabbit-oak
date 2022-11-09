package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

public class TopKPropertyInfo {

	private final String name;
	private final long count;

	public TopKPropertyInfo(String name, long count) {
		this.name = name;
		this.count = count;
	}

	public long getCount() {
		return count;
	}

	public String getName() {
		return name;
	}

//	private String entry(String name, long val) {
//		JsopBuilder.encode(name);
//		return name + " : " + val;
//	}
	@Override
	public String toString() {
		return "(" + name + ", " + count + ")";
	}
}
