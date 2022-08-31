package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;

public class PropertyStatistics {

	private final String name;
	private long count;
	private CountMinSketch valueSketch;
	private TopKElements topKElements;
	private HyperLogLog hll;

	PropertyStatistics(String name, long count, HyperLogLog hll) {
		this.name = name;
		this.count = count;
		this.hll = hll;
	}

	// which data structure to store top k counts?
	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
			TopKElements topKElements) {
		this(name, count, hll);
		this.valueSketch = valueSketch;
		this.topKElements = topKElements;
	}

	void updateHll(long hash) {
		hll.add(hash);
	}

	void updateValueCounts(Object val, long hash) {
		valueSketch.add(hash);
		topKElements.update(val, valueSketch.estimateCount(hash));
	}

	List<PropertyInfo> getTopKValuesDescending() {
		return topKElements.get();
	}

	CountMinSketch getValueSketch() {
		return new CountMinSketch(valueSketch);
	}

	long getCount() {
		return count;
	}

	String getName() {
		return name;
	}

	HyperLogLog getHll() {
		return hll;
	}

	void inc(long count) {
		this.count += count;
	}

}
