package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.List;
import java.util.Optional;

import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsIndexHelper.getStringOrEmpty;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;

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

    public PropertyStatistics(String name, long count, HyperLogLog hll,
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

    /**
     * Gets one instance of PropertyStatistics from the index node. Assumes that
     * propertyNode is stored at:
     * oak:index/statistics/index/properties/{@code propertyNode}. At the
     * moment, if one of the stored properties are corrupted, we will just
     * create that PropertyStatistics with a zero, empty string or empty array
     * (where appropriate) parameter.
     * <p>
     * TODO: should we create a new completely new PropertyStatistics if
     * reading one of the properties fails?
     *
     * @param name         the propertyNode name we want to get the statistics
     *                     from
     * @param propertyNode the propertyNode node stored at the path
     *                     oak:index/statistics/index/properties/
     * @return A PropertyStatistics that contains the stored or default values.
     */
    public static Optional<PropertyStatistics> fromPropertyNode(String name,
                                                                NodeState propertyNode) {

        if (!propertyNode.exists() || !propertyNode.hasChildNode(name)) {
            return Optional.empty();
        }

        NodeState dataNode = propertyNode.getChildNode(name);

        long count = getLong(dataNode, StatisticsEditor.EXACT_COUNT);

        CountMinSketch cms = CountMinSketch.readCMS(dataNode,
                                                    StatisticsEditor.VALUE_SKETCH_NAME,
                                                    StatisticsEditor.VALUE_SKETCH_ROWS,
                                                    StatisticsEditor.VALUE_SKETCH_COLS,
                                                    ContentStatistics.CS_LOG);
        String storedHll = getStringOrEmpty(dataNode,
                                            StatisticsEditor.PROPERTY_HLL_NAME);

        byte[] hllData = HyperLogLog.deserialize(storedHll);
        HyperLogLog hll = new HyperLogLog(hllData.length, hllData);

        int topK = Math.toIntExact(
                getLong(dataNode, StatisticsEditor.PROPERTY_TOP_K));

        TopKValues topKValues = StatisticsEditor.readTopKElements(
                dataNode.getProperty(StatisticsEditor.PROPERTY_TOP_K_NAME),
                dataNode.getProperty(StatisticsEditor.PROPERTY_TOP_K_COUNT),
                topK);


        long valueLengthTotal = getLong(dataNode,
                                        StatisticsEditor.VALUE_LENGTH_TOTAL);
        long valueLengthMax = getLong(dataNode,
                                      StatisticsEditor.VALUE_LENGTH_MAX);
        long valueLengthMin = getLong(dataNode,
                                      StatisticsEditor.VALUE_LENGTH_MIN);

        return Optional.of(
                new PropertyStatistics(name, count, hll, cms, topKValues,
                                       valueLengthTotal, valueLengthMax,
                                       valueLengthMin));
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

    public List<TopKValues.ValueCountPair> getTopKValuesDescending() {
        return topKValues.get();
    }

    CountMinSketch getValueSketch() {
        return new CountMinSketch(valueSketch);
    }

    public long getValueLengthTotal() {
        return valueLengthTotal;
    }

    public long getValueLengthMax() {
        return valueLengthMax;
    }

    public long getValueLengthMin() {
        return valueLengthMin;
    }

    public long getCount() {
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

    /**
     * Gets the estimated number of unique values this property has.
     *
     * @return the estimated number of unique values of this property.
     */
    public long getUniqueCount() {
        return hll.estimate();
    }

    /**
     * Gets the estimated count of the value, e.g "red" or "blue" if the
     * property is "color".
     *
     * @param value the value to get the estimated count of. E.g. "blue" or
     *              "red"
     * @return the estimated count of the value under this property.
     */
    public long getCmsCount(String value) {
        long hash = Hash.hash64(value.hashCode());
        return valueSketch.estimateCount(hash);
    }

    @Override
    public String toString() {

        JsopBuilder builder = new JsopBuilder();
        builder.object();

        builder = builder.key("propertyName");
        builder = builder.value(name);

        builder = builder.key("count");
        builder = builder.value(count);

        builder = builder.key("hllCount");
        builder = builder.value(getUniqueCount());

        builder = builder.key("valueLengthTotal");
        builder = builder.value(valueLengthTotal);

        builder = builder.key("valueLengthMax");
        builder = builder.value(valueLengthMax);

        builder = builder.key("valueLengthMin");
        builder = builder.value(valueLengthMin);

        builder = builder.endObject();

        return builder.toString();
    }
}
