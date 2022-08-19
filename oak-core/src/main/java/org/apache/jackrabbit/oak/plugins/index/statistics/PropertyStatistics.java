package org.apache.jackrabbit.oak.plugins.index.statistics;

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

	void updateValueCounts(long hash) {
		valueSketch.add(hash);
	}

	String getSortedTopKElements() {
		if (topKElements.isEmpty()) {
			return "";
		}
		if (topKElements.contains(name)) {
			// update existing value and its corresponding count
			topKElements.update(name, count);
		} else if (topKElements.minElement() < count) {
			// it does not exist in the topKElements and we should replace it.
			topKElements.removeMinElement();
			topKElements.add(name, count);
		}

//		List<PropertyInfo> sortedElements = topKElements.getAllSorted();

		return topKElements.serialize();
//		for (Long l : topKCounts) {
//			res.add(l);
//		}

//
//		return Arrays.copyOf(topKCounts, topKCounts.length);
	}

//	String[] getTopKValues() {
//		return Arrays.stream(topKValues).toArray(String[]::new);
//	}

	String[] readTopKValues(String storedValues) {
		return new String[10];
	}

	long getCount() {
		return count;
	}

	HyperLogLog getHll() {
		return hll;
	}

	void inc(long count) {
		this.count += count;
	}

	public void updateTopElements(String propertyName) {
		// TODO Auto-generated method stub
		topKElements.update(propertyName, 1);

	}
}
