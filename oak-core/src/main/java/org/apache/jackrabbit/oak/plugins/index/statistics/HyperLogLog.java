package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HyperLogLog implements CardinalityEstimator {
    private static final Logger logger = LoggerFactory.getLogger(
            HyperLogLog.class);
    private final int m;
    private final byte[] counters;
    private final double am;

    public HyperLogLog(int m) {
        this(m, new byte[m]);
    }

    public HyperLogLog(int m, byte[] counters) {
        if (m < 16) {
            throw new IllegalArgumentException(
                    "The size must be >= 16. The provided size is: " + m);
        }
        if (Integer.bitCount(m) != 1) {
            throw new IllegalArgumentException(
                    "The size must be a power of 2. The provided size is: " + m);
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

    /**
     * Reads the HyperLogLog.toString() representation that was stored at
     * oak:index/statistics.
     *
     * @param hllCounts a space separated string of bytes
     * @return a byte[] that contains the HyperLogLog information.
     */
    public static byte[] deserialize(String hllCounts) {
        String[] parts = hllCounts.split("\\s+");
        if (parts.length != StatisticsEditor.DEFAULT_HLL_SIZE) {
            logger.warn(
                    "Number of elements in: " + hllCounts + " does not match "
                            + "the " + "expected: " + StatisticsEditor.DEFAULT_HLL_SIZE);
            return new byte[StatisticsEditor.DEFAULT_HLL_SIZE];
        }
        
        try {
            List<Byte> longList = Stream.of(parts)
                                        .map(Byte::valueOf)
                                        .collect(Collectors.toList());
            byte[] data = new byte[parts.length];
            for (int i = 0; i < longList.size(); i++) {
                data[i] = longList.get(i);
            }
            return data;
        } catch (NumberFormatException e) {
            logger.warn("Can not parse: " + hllCounts);
        }

        return new byte[StatisticsEditor.DEFAULT_HLL_SIZE];
    }

    @Override
    public void add(long hash) {
        int i = (int) (hash & (m - 1));
        counters[i] = (byte) Math.max(counters[i],
                                      1 + Long.numberOfLeadingZeros(hash));
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
        if (est <= 5L * m && countZero > 0) {
            // linear counting
            est = (long) (m * Math.log((double) m / countZero));
        }
        return Math.max(1, est);
    }

    /**
     * Converts the HyperLogLog information into a string so that we can store
     * it under the statistics index.
     *
     * @return a space separated string of bytes
     */
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

    public byte[] getCounters() {
        return counters;
    }

}