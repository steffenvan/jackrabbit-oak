package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HyperLogLog implements CardinalityEstimator {

	private final int m;
	private byte[] counts;
	private final double am;

	public HyperLogLog(int m) {
		this.m = m;
		this.counts = new byte[m];
		switch (m) {
		case 32:
			this.am = 0.697;
			break;
		case 64:
			this.am = 0.709;
			break;
		default:
			this.am = 0.7213 / (1.0 + 1.079 / m);
		}
	}

	@Override
	public void add(long hash) {
		int i = (int) (hash & (m - 1));
		counts[i] = (byte) Math.max(counts[i], 1 + Long.numberOfLeadingZeros(hash));
	}

	@Override
	public long estimate() {
		int numZeroes = 0;
		double sum = 0.0;
		for (int count : counts) {
			sum += 1.0 / (1L << (count & 0xff));
			if (count == 0) {
				numZeroes++;
			}
		}

		double estimate = 1 / am * m * m * sum;

		if (estimate <= 5 * m && numZeroes > 0) {
			estimate = linearCount(numZeroes);
		}
		return (long) Math.max(1, estimate);
	}

	private double linearCount(int numZeroes) {
		return m * Math.log((double) m / numZeroes);
	}

	byte[] getCounts() {
		return Arrays.copyOf(counts, m);
	}

	public String serialize() {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < counts.length; i++) {
			if (i > 0) {
				sb.append(" ");
			}
			sb.append(counts[i]);
		}

		return sb.toString();
	}

	public byte[] deserialize(String hllCounts) {
		String[] parts = hllCounts.split("\\s+");
		List<Byte> longList = Stream.of(parts).map(Byte::valueOf).collect(Collectors.toList());
		byte[] data = new byte[parts.length];
		for (int i = 0; i < longList.size(); i++) {
			data[i] = longList.get(i);
		}
		return data;
	}
}
