package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.List;

/**
 * Represents the collected statistics data for a property. This object is only
 * created for properties whose count exceeds a certain threshold. Note that
 * this count is different from the *value* count.
 */
public class PropertyStatistics {
    private final String name;
    private final CountMinSketch valueSketch;
    private final TopKValues topKValues;
    private final HyperLogLog hll;

    /*
    this represents the "true" count and not the estimated Count-MinSketch count
    */
    private long count;
    private long valueLengthTotal;
    private long valueLengthMax;
    private long valueLengthMin;

    PropertyStatistics(String name, long count, HyperLogLog hll,
                       CountMinSketch valueSketch, TopKValues topKValues) {
        this(name, count, hll, valueSketch, topKValues, 0, 0, Long.MAX_VALUE);

    }

    PropertyStatistics(String name, long count, HyperLogLog hll,
                       CountMinSketch valueSketch, TopKValues topKValues,
                       long valueLengthTotal, long valueLengthMax,
                       long valueLengthMin) {
        this.name = name;
        this.count = count;
        this.hll = hll;
        this.valueSketch = valueSketch;
        this.topKValues = topKValues;
        this.valueLengthTotal = valueLengthTotal;
        this.valueLengthMax = valueLengthMax;
        this.valueLengthMin = valueLengthMin;
    }

    void updateHll(long hash) {
        hll.add(hash);
    }

    /**
     * Updates the count of the provided value. Since we update the value count,
     * we also need to update the top K values object because the updated
     * value's count might exceed the count of the lowest of the current top K.
     *
     * @param val    the value being modified or added
     * @param hash64 the value's 64-bit hash value
     */
    void updateValueCount(String val, long hash64) {
        valueSketch.add(hash64);
        topKValues.update(val, valueSketch.estimateCount(hash64));
    }

    void updateValueLength(String val) {
        long len = val.length();
        valueLengthTotal += len;
        valueLengthMax = Math.max(valueLengthMax, len);
        valueLengthMin = Math.min(valueLengthMin, len);
    }

    List<TopKValues.ValueCountPair> getTopKValuesDescending() {
        return topKValues.get();
    }

    CountMinSketch getValueSketch() {
        return new CountMinSketch(valueSketch);
    }

    long getValueLengthTotal() {
        return valueLengthTotal;
    }

    long getValueLengthMax() {
        return valueLengthMax;
    }

    long getValueLengthMin() {
        return valueLengthMin;
    }

    long getCount() {
        return count;
    }

    String getName() {
        return name;
    }

    HyperLogLog getHll() {
        return hll;
    }

    void incCount(long count) {
        this.count += count;
    }

    public long getCmsCount(long hash) {
        return valueSketch.estimateCount(hash);
    }
}
