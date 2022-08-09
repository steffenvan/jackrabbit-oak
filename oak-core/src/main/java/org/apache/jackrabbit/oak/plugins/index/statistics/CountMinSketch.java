package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountMinSketch implements FrequencyCounter {
	private final long[][] items;
	private final int rows;
	private final int cols;

	private final double epsilon;

	private final double delta;

	private final int shift;

	private long count;

	private static final Random RANDOM = new Random();

	public CountMinSketch(int rows, int cols) {
		this(rows, cols, new long[rows][cols]);
	}

	public CountMinSketch(double epsilon, double delta) {
		this.epsilon = epsilon;
		this.delta = delta;
		this.cols = (int) Math.ceil(2 / epsilon);
		this.rows = (int) Math.ceil(-Math.log(1 - delta) / Math.log(2));
		this.shift = Integer.bitCount(this.rows);
		items = new long[this.rows][this.cols];
	}

	public CountMinSketch(int rows, int cols, long[][] items) {
		this.rows = rows;
		this.cols = cols;
		this.epsilon = 2.0 / cols;
		this.delta = 1 - 1 / Math.pow(2, rows);
		this.shift = Integer.bitCount(rows);
		this.items = items;
	}

	void setSeed(int seed) {
		RANDOM.setSeed(seed);
	}

	@Override
	public void add(long hash) {
		count++;
		for (int i = 0; i < rows; i++) {
			int col = Hash.reduce(hash, cols);
			items[i][col]++;
			hash >>>= shift;
		}
	}

	@Override
	public long estimateCount(long hash) {
		long currMin = Long.MAX_VALUE;
		for (int i = 0; i < rows; i++) {
			currMin = Math.min(currMin, items[i][Hash.reduce(hash, cols)]);
			hash >>>= shift;
		}

		return currMin;
	}

	public long[][] getData() {
	    return items;
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

	public long[] deserialize(String row) {
		String[] allStuff = row.split("\\s+");
		List<Long> longList = Stream.of(allStuff).map(Long::valueOf).collect(Collectors.toList());
		return longList.stream().mapToLong(i -> i).toArray();
	}
}
