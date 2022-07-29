package org.apache.jackrabbit.oak.plugins.index.statistics;

public class PropertyStatistics {

    private final String name;
    private long count;
    private HyperLogLog hll;

    PropertyStatistics(String name, long count, HyperLogLog hll) {
        this.name = name;
        this.count = count;
        this.hll = hll;
    }

    void updateHll(long hash) {
        hll.add(hash);
    }

    long getCount() {
        return count;
    }
    
    HyperLogLog getHll() {
    	return hll;
    }

    void inc(long count) {
        this.count += count;
    }
}
