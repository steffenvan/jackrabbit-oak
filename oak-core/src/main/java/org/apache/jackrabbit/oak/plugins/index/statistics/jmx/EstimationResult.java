package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKValues;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class EstimationResult {
    private final String propertyName;
    private final long count;
    private final long cmsCount;
    private final long hllCount;
    private final long valueLengthTotal;
    private final long valueLengthMax;
    private final long valueLengthMin;
    private final TopKValues topKValues;

    public EstimationResult(String propertyName, long count, long cmsCount, long hllCount, long valueLengthTotal, long valueLengthMax, long valueLengthMin, TopKValues topKValues) {
        this.propertyName = propertyName;
        this.count = count;
        this.cmsCount = cmsCount;
        this.hllCount = hllCount;
        this.valueLengthTotal = valueLengthTotal;
        this.valueLengthMax = valueLengthMax;
        this.valueLengthMin = valueLengthMin;
        this.topKValues = topKValues;
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

    public String getTopKValues() {
        return topKValues.get().toString();
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

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
//        String result = "{";
////        JsopBuilder.encode()
//        result += entry("count", count);
//        result += ", ";
//        result += entry("topKValues", topValues);
//        result += ", ";
//        result += entry("propertyName", propertyName);
//        result += ", ";
//        result += entry("cmsCount", cmsCount);
//        result += ", ";
//        result += entry("hll", hllCount);
//        result += ", ";
//        result += entry("valueLengthTotal", valueLengthTotal);
//        result += ", ";
//        result += entry("valueLengthMax", valueLengthMax);
//        result += ", ";
//        result += entry("valueLengthMin", valueLengthMin);
//        result += "}";
//        return result;
    }

    private String entry(String name, String val) {
        return name + ":" + val;
//        return quotes ? "\"" + name + "\"" + " : " + "\"" + val + "\"" : "\"" + name + "\"" + " : " + val;
    }

    private String entry(String name, long val) {
        return name + ":" + val;
    }

    private String entry(String name, PriorityQueue<TopKValues.ValueCountPair> val) {
        return "\"" + name + "\"" + " : " + val;
    }

    public static void main(String[] args) {
        long avgLength = 400L;
        long maxLength = 30000L;
        long minLength = 200L;
        List<TopKValues.ValueCountPair> values = new ArrayList<>();

        String test = "\"fooBaz";
//        values.add(new TopKValues.ValueCountPair(test, 32L));
        String name_s = "function check() { var path = workflowData.getPayload().toString(); var node = jcrSession.getNode(path); return 'fai... [577854073]";
//        values.add(new TopKValues.ValueCountPair(name_s, 10));
        String s = "";
//        EstimationResult er = new EstimationResult("jcr:primaryType", 42, 32, 1, avgLength, maxLength, minLength, values);
//        System.out.println(er);
    }
}
