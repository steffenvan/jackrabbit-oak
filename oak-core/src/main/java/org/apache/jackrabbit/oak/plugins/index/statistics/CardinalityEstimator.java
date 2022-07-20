package org.apache.jackrabbit.oak.plugins.index.statistics;

public interface CardinalityEstimator {
    void add(long hash);
    long estimate();
}
