package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKValues;

import java.util.List;

public class EstimationResult {
    private final String propertyName;
    private final long count;
    private final long cmsCount;
    private final long hllCount;
    private final long valueLengthTotal;
    private final long valueLengthMax;
    private final long valueLengthMin;
    private final List<TopKValues.ValueCountPair> topKValuesDescending;

    public EstimationResult(String propertyName, long count, long cmsCount,
                            long hllCount, long valueLengthTotal,
                            long valueLengthMax, long valueLengthMin,
                            List<TopKValues.ValueCountPair> topKValuesDescending) {
        this.propertyName = propertyName;
        this.count = count;
        this.cmsCount = cmsCount;
        this.hllCount = hllCount;
        this.valueLengthTotal = valueLengthTotal;
        this.valueLengthMax = valueLengthMax;
        this.valueLengthMin = valueLengthMin;
        this.topKValuesDescending = topKValuesDescending;
    }

    public long getCount() {
        return this.count;
    }

    public long getCmsCount() {
        return this.cmsCount;
    }

    public long getHllCount() {
        return this.hllCount;
    }

    public String getName() {
        return propertyName;
    }

    @Override
    public String toString() {

        JsopBuilder builder = new JsopBuilder();
        builder.object();

        builder = builder.key("propertyName");
        builder = builder.value(propertyName);

        builder = builder.key("count");
        builder = builder.value(count);

        builder = builder.key("cmsCount");
        builder = builder.value(cmsCount);

        builder = builder.key("hllCount");
        builder = builder.value(hllCount);

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
