package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.jackrabbit.oak.plugins.index.statistics.TopKElements;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKPropertyInfo;

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
    private final String topValues;

    public EstimationResult(String propertyName, long count, long cmsCount, long hllCount, long valueLengthTotal, long valueLengthMax, long valueLengthMin, String topValues) {
        this.propertyName = propertyName;
        this.count = count;
        this.cmsCount = cmsCount;
        this.hllCount = hllCount;
        this.valueLengthTotal = valueLengthTotal;
        this.valueLengthMax = valueLengthMax;
        this.valueLengthMin = valueLengthMin;
        this.topValues = topValues;
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
        String result = "{";
        result += entry("propertyName", propertyName, true);
        result += ", ";
        result += entry("count", count);
        result += ", ";
        result += entry("cmsCount", cmsCount);
        result += ", ";
        result += entry("hll", hllCount);
        result += ", ";
        result += entry("valueLengthTotal", valueLengthTotal);
        result += ", ";
        result += entry("valueLengthMax", valueLengthMax);
        result += ", ";
        result += entry("valueLengthMin", valueLengthMin);
        result += ", ";
        result += entry("topKValues", topValues, false);
        result += "}";
        return result;
    }

    private String entry(String name, String val, boolean quotes) {
        return quotes ? "\"" + name + "\"" + " : " + "\"" + val + "\"" : "\"" + name + "\"" + " : " + val;
    }

    private String entry(String name, long val) {
        return "\"" + name + "\"" + " : " + val;
    }

    private String entry(String name, PriorityQueue<TopKElements.ValueCountPair> val) {
        return "\"" + name + "\"" + " : " + val;
    }

    public static void main(String[] args) {
        long avgLength = 400L;
        long maxLength = 30000L;
        long minLength = 200L;
        List<TopKPropertyInfo> values = new ArrayList<>();

        String test = "\"fooBaz";
        values.add(new TopKPropertyInfo(test, 32L));
        String name_s = "function check() { var path = workflowData.getPayload().toString(); var node = jcrSession.getNode(path); return 'fai... [577854073]";
        values.add(new TopKPropertyInfo(name_s, 10));
        String s = "";
        EstimationResult er = new EstimationResult("jcr:primaryType", 42, 32, 1, avgLength, maxLength, minLength, s);
        System.out.println(er);
    }
//    {"propertyName" : "jcr:primaryType", "cms" : "42", "hll" : "1"}

}
