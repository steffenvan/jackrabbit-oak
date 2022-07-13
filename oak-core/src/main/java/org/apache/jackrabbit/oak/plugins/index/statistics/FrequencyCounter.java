package org.apache.jackrabbit.oak.plugins.index.statistics;

public interface FrequencyCounter {

    void add(long hash);
    long estimateCount(long hash);
}
