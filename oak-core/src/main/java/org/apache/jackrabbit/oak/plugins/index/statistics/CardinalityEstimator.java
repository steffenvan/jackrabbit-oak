package org.apache.jackrabbit.oak.plugins.index.statistics;

/**
 * Used to estimate the number of unique values that a property has. We
 * instantiate a CardinalityEstimator each property whose frequency count
 * exceeds a certain threshold. Consequently, each CardinalityEstimator contains
 * the number of unique values for a property.
 */
public interface CardinalityEstimator {
    /**
     * Adds a value's hashcode representation to the estimator's internal data
     * data structure.
     *
     * @param hash a 64-bit hash value.
     */
    void add(long hash);

    /**
     * Get the number of unique values for a property.
     *
     * @return the estimated number of unique values
     */
    long estimate();
}
