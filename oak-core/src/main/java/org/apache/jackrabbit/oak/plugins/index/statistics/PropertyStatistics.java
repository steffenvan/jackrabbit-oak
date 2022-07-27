package org.apache.jackrabbit.oak.plugins.index.statistics;

public class PropertyStatistics {

    private final String name;
    private long count;
    private HyperLogLog hll;
    // skewed distribution?

    PropertyStatistics(String name, long count, HyperLogLog hll) {
        this.name = name;
        this.count = count;
        this.hll = hll;
    }

    void updateStats(long hash) {
        hll.add(hash);
    }

    long getCount() {
        return count;
    }

    void inc(long count) {
        this.count += count;
    }
}
