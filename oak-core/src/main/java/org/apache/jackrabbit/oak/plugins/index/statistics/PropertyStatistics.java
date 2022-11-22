package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;

import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getLongOrZero;

public class PropertyStatistics {

	private final String name;
	private long count;
	private final CountMinSketch valueSketch;
	private final TopKValues topKValues;
	private final HyperLogLog hll;
	private long valueLengthTotal;
	private long valueLengthMax;
	private long valueLengthMin;

	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
			TopKValues topKValues) {
		this(name, count, hll, valueSketch, topKValues, 0, 0, Long.MAX_VALUE);

	}

	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
					   TopKValues topKValues, long valueLengthTotal, long valueLengthMax, long valueLengthMin) {
		this.name = name;
		this.count = count;
		this.hll = hll;
		this.valueSketch = valueSketch;
		this.topKValues = topKValues;
		this.valueLengthTotal = valueLengthTotal;
		this.valueLengthMax = valueLengthMax;
		this.valueLengthMin = valueLengthMin;
	}

	void updateHll(long hash) {
		hll.add(hash);
	}

	void updateValueCounts(String val, long hash) {
		valueSketch.add(hash);
		topKValues.update(val, valueSketch.estimateCount(hash));
		long len = val.length();
		valueLengthTotal += len;
		valueLengthMax = Math.max(valueLengthMax, len);
		valueLengthMin = Math.min(valueLengthMin, len);
	}

	List<TopKValues.ValueCountPair> getTopKValuesDescending() {
		return topKValues.get();
	}

	CountMinSketch getValueSketch() {
		return new CountMinSketch(valueSketch);
	}

	long getValueLengthTotal() {
		return valueLengthTotal;
	}

	long getValueLengthMax() {
		return valueLengthMax;
	}

	long getValueLengthMin() {
		return valueLengthMin;
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

	public long getCmsCount(long hash) {
		return valueSketch.estimateCount(hash);
	}
}
