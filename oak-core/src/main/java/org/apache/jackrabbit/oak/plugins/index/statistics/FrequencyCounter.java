package org.apache.jackrabbit.oak.plugins.index.statistics;

/**
 * Represents the approximate count of properties or values. We only count the
 * values if their respective property exceeds a certain threshold.
 *
 */
public interface FrequencyCounter {

    /**
     * Adds a certain property or value into the data structure.
     *
     * @param hash a 64 bit hash value.
     */
    void add(long hash);

    /**
     * Gets the estimated count of the provided property or value.
     *
     * @param hash a 64 bit hash value.
     * @return the estimated count of property or value.
     */
    long estimateCount(long hash);
}
