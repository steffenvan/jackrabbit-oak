package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getLongOrZero;

public class CountMinSketch implements FrequencyCounter {
	private static final Random RANDOM = new Random();
	private final long[][] items;
	private final int rows;
	private final int cols;
	private long count;

	public CountMinSketch(int rows, int cols) {
		this(rows, cols, new long[rows][cols]);
	}

	public CountMinSketch(int rows, int cols, long[][] items) {
		if (Integer.bitCount(cols) != 1 || cols < 2) {
			// throw new IllegalArgumentException("The number of columns must be a power of 2 and larger than 2.");
			System.out.println("error");
		}

		this.rows = rows;
		this.cols = cols;
		this.items = items;
	}

	// Copy constructor
	public CountMinSketch(CountMinSketch cms) {
		this(cms.getRows(), cms.getCols(), cms.getData());
	}

	public static long estimateCount(long hash, long[][] counts, int cols,
									 int rows) {
		long currMin = Long.MAX_VALUE;
		for (int i = 0; i < rows; i++) {
			long h2 = Hash.hash64(hash, i);
			int col = (int) (h2 & (cols - 1));
			currMin = Math.min(currMin, counts[i][col]);
		}

		return currMin;
	}

	public static long[] deserialize(String row) {
		String[] allStuff = row.split("\\s+");
		List<Long> longList = Stream.of(allStuff).map(Long::valueOf).collect(Collectors.toList());
		return longList.stream().mapToLong(i -> i).toArray();
	}

	public static CountMinSketch readCMS(NodeState node, String cmsName,
										 String rowName, String colName,
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
	public int getRows() {
		return this.rows;
	}

	public int getCols() {
		return this.cols;
	}

	@Override
	public void add(long hash) {
		for (int i = 0; i < rows; i++) {
			long h2 = Hash.hash64(hash, i);
			int col = (int) (h2 & (cols - 1));
			items[i][col]++;
		}
		count++;
	}

	@Override
	public long estimateCount(long hash) {
		long currMin = Long.MAX_VALUE;
		for (int i = 0; i < rows; i++) {
			long h2 = Hash.hash64(hash, i);
			int col = (int) (h2 & (cols - 1));
			currMin = Math.min(currMin, items[i][col]);
		}

		return currMin;
	}

	public long[][] getData() {
		return Arrays.stream(items).map(long[]::clone).toArray(long[][]::new);
	}

	public String[] serialize() {
		StringBuilder sb = new StringBuilder();
		String[] asStrings = new String[rows];
		for (int i = 0; i < rows; i++) {
			for (int j = 0; j < cols; j++) {
				if (j > 0) {
					sb.append(" ");
				}
				sb.append(items[i][j]);
			}
			asStrings[i] = sb.toString();
			sb = new StringBuilder();
		}

		return asStrings;
	}

	void setSeed(int seed) {
		RANDOM.setSeed(seed);
	}
}
