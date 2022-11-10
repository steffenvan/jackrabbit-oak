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
	private final TopKElements topKElements;
	private final HyperLogLog hll;
	private long valueLengthTotal;
	private long valueLengthMax;
	private long valueLengthMin;

	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
			TopKElements topKElements) {
		this(name, count, hll, valueSketch, topKElements, 0, 0, Long.MAX_VALUE);

	}

	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
					   TopKElements topKElements, long valueLengthTotal, long valueLengthMax, long valueLengthMin) {
		this.name = name;
		this.count = count;
		this.hll = hll;
		this.valueSketch = valueSketch;
		this.topKElements = topKElements;
		this.valueLengthTotal = valueLengthTotal;
		this.valueLengthMax = valueLengthMax;
		this.valueLengthMin = valueLengthMin;
	}

	void updateHll(long hash) {
		hll.add(hash);
	}

	void updateValueCounts(String val, long hash) {
		valueSketch.add(hash);
		topKElements.update(val, valueSketch.estimateCount(hash));
		long len = val.length();
		valueLengthTotal += len;
		valueLengthMax = Math.max(valueLengthMax, len);
		valueLengthMin = Math.min(valueLengthMin, len);
	}

	List<TopKElements.ValueCountPair> getTopKValuesDescending() {
		return topKElements.get();
	}

	CountMinSketch getValueSketch() {
		return new CountMinSketch(valueSketch);
	}

	public static CountMinSketch readCMS(NodeState node, String cmsName, String rowName, String colName,
			Logger logger) {
		PropertyState rowsProp = node.getProperty(rowName);
		PropertyState colsProp = node.getProperty(colName);

		int rows = getLongOrZero(rowsProp).intValue();
		int cols = getLongOrZero(colsProp).intValue();
		long[][] data = new long[rows][cols];

		for (int i = 0; i < rows; i++) {
			PropertyState ps = node.getProperty(cmsName + i);
			if (ps != null) {
				String s = ps.getValue(Type.STRING);
				try {
					long[] row = CountMinSketch.deserialize(s);
					data[i] = row;
				} catch (NumberFormatException e) {
					logger.warn("Can not parse " + s);
				}
			}
		}

		return new CountMinSketch(rows, cols, data);
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
