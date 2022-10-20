package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;

public class PropertyStatistics {

	private final String name;
	private long count;
	private CountMinSketch valueSketch;
	private TopKElements topKElements;
	private HyperLogLog hll;

	PropertyStatistics(String name, long count, HyperLogLog hll, CountMinSketch valueSketch,
			TopKElements topKElements) {
        this.name = name;
        this.count = count;
        this.hll = hll;
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

	public static CountMinSketch readCMS(NodeState node, String cmsName, String rowName, String colName,
			Logger logger) {
		PropertyState rowsProp = node.getProperty(rowName);
		PropertyState colsProp = node.getProperty(colName);

		int rows = rowsProp.getValue(Type.LONG).intValue();
		int cols = colsProp.getValue(Type.LONG).intValue();
		long[][] data = new long[rows][cols];

        if (rowsProp != null || colsProp != null) {
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
        }

		return new CountMinSketch(rows, cols, data);
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
