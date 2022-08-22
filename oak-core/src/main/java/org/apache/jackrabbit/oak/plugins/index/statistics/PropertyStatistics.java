package org.apache.jackrabbit.oak.plugins.index.statistics;

public class PropertyStatistics {

	private final String name;
	private long count;
	private CountMinSketch valueSketch;

//	PriorityQueue<PropertyInfo> topElements;
//	private Map<String, Long> valuesToCounts;
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
//		this.valuesToCounts = valuesToCounts;
	}

	void updateHll(long hash) {
		hll.add(hash);
	}

	void updateValueCounts(Object val, long hash) {
		valueSketch.add(hash);
		topKElements.update(val, valueSketch.estimateCount(hash));
	}

	String getSortedTopKElements() {
		return topKElements.serialize();
	}

	String[] readTopKValues(String storedValues) {
		return new String[10];
	}

	long getCount() {
		return count;
	}

	void clear() {
		topKElements.clear();
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
