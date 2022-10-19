package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HyperLogLog implements CardinalityEstimator {

    private final int m;
    private byte[] counters;
    private final double am;

    public HyperLogLog(int m) {
        this(m, new byte[m]);
    }

    public HyperLogLog(int m, byte[] counters) {
        if (m < 16) {
            throw new IllegalArgumentException("Must be >= 16, is " + m);
        }
        if (Integer.bitCount(m) != 1) {
            throw new IllegalArgumentException("Must be a power of 2, is " + m);
        }
        this.m = m;
        this.counters = counters;
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
        counters[i] = (byte) Math.max(counters[i], 1 + Long.numberOfLeadingZeros(hash));
    }

    @Override
    public long estimate() {
        double sum = 0;
        int countZero = 0;
        for (int c : counters) {
            countZero += c == 0 ? 1 : 0;
            sum += 1. / (1L << (c & 0xff));
        }
        long est = (long) (1. / sum * am * m * m);
        if (est <= 5 * m && countZero > 0) {
            // linear counting
            est = (long) (m * Math.log((double) m / countZero));
        }
        return Math.max(1, est);
    }

    public String serialize() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < counters.length; i++) {
            if (i > 0) {
                sb.append(" ");
            }
            sb.append(counters[i]);
        }

        return sb.toString();
    }

    public static byte[] deserialize(String hllCounts) {
        String[] parts = hllCounts.split("\\s+");
        List<Byte> longList = Stream.of(parts).map(Byte::valueOf).collect(Collectors.toList());
        byte[] data = new byte[parts.length];
        for (int i = 0; i < longList.size(); i++) {
            data[i] = longList.get(i);
        }
        return data;
    }

    public byte[] getCounters() {
        return counters;
    }

}