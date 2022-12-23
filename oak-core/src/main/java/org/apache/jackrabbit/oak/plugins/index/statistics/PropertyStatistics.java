package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.List;
import java.util.Optional;

import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexReader.getIndexRoot;
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexReader.getStatisticsIndexDataNodeOrNull;
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexReader.getStringOrEmpty;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor.PROPERTIES;
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

    public static Optional<NodeState> getPropertyNodeState(String name,
                                                           NodeStore store) {
        Optional<NodeState> statisticsDataNode =
                getStatisticsIndexDataNodeOrNull(
                getIndexRoot(store));

        if (statisticsDataNode.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(statisticsDataNode.get()
                                             .getChildNode(PROPERTIES)
                                             .getChildNode(name));
    }

    public static Optional<PropertyStatistics> fromName(String name,
                                                        NodeStore store) {

        Optional<NodeState> properties = getPropertyNodeState(name, store);
        if (properties.isEmpty()) {
            return Optional.empty();
        }

        return fromNodeState(name, properties.get());
    }

    public static Optional<PropertyStatistics> fromNodeState(String name,
                                                             NodeState property) {
        long count = getLong(property, StatisticsEditor.EXACT_COUNT);
        long valueLengthTotal = getLong(property,
                                        StatisticsEditor.VALUE_LENGTH_TOTAL);
        long valueLengthMax = getLong(property,
                                      StatisticsEditor.VALUE_LENGTH_MAX);
        long valueLengthMin = getLong(property,
                                      StatisticsEditor.VALUE_LENGTH_MIN);


        String storedHll = getStringOrEmpty(property,
                                            StatisticsEditor.PROPERTY_HLL_NAME);
        byte[] hllData = HyperLogLog.deserialize(storedHll);
        HyperLogLog hll = new HyperLogLog(hllData.length, hllData);

        int topK = Math.toIntExact(
                getLong(property, StatisticsEditor.PROPERTY_TOP_K));
        TopKValues topKValues = StatisticsEditor.readTopKElements(
                property.getProperty(StatisticsEditor.PROPERTY_TOP_K_NAME),
                property.getProperty(StatisticsEditor.PROPERTY_TOP_K_COUNT),
                topK);

        CountMinSketch cms = CountMinSketch.readCMS(property,
                                                    StatisticsEditor.VALUE_SKETCH_NAME,
                                                    StatisticsEditor.VALUE_SKETCH_ROWS,
                                                    StatisticsEditor.VALUE_SKETCH_COLS,
                                                    ContentStatistics.CS_LOG);

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

    public long getUniqueCount() {
        return hll.estimate();
    }

    void incCount(long count) {
        this.count += count;
    }

    public long getCmsCount() {
        long hash = Hash.hash64(name.hashCode());
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

        builder = builder.key("cmsCount");
        builder = builder.value(getCmsCount());

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
