package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CountMinSketch implements FrequencyCounter {
	private final long[][] items;
	private final int rows;
	private final int cols;
	private final int shift;

	private long count;

	private static final Random RANDOM = new Random();

	public CountMinSketch(int rows, int cols) {
		this(rows, cols, new long[rows][cols]);
	}

	public CountMinSketch(int rows, int cols, long[][] items) {
		this.rows = rows;
		this.cols = cols;
		this.shift = Integer.bitCount(rows);
		this.items = items;
	}

	public CountMinSketch(CountMinSketch cms) {
		this(cms.getRows(), cms.getCols(), cms.getData());
	}

	void setSeed(int seed) {
		RANDOM.setSeed(seed);
	}

	public int getRows() {
		return this.rows;
	}

	public int getCols() {
		return this.cols;
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

	public static long estimateCount(long hash, long[][] counts, int cols, int rows) {
		long currMin = Long.MAX_VALUE;
		int shift = Integer.bitCount(rows);
		for (int i = 0; i < rows; i++) {
			currMin = Math.min(currMin, counts[i][Hash.reduce(hash, cols)]);
			hash >>>= shift;
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

	public static long[] deserialize(String row) {
		String[] allStuff = row.split("\\s+");
		List<Long> longList = Stream.of(allStuff).map(Long::valueOf).collect(Collectors.toList());
		return longList.stream().mapToLong(i -> i).toArray();
	}
}
