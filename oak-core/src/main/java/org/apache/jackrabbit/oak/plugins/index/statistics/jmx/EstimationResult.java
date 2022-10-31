package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

public class EstimationResult {
    private final String propertyName;
    private final long count;
    private final long hllCount;
    private final long valueLengthTotal;
    private final long valueLengthMax;
    private final long valueLengthMin;

    public EstimationResult(String propertyName, long count, long hllCount, long valueLengthTotal, long valueLengthMax, long valueLengthMin) {
        this.propertyName = propertyName;
        this.count = count;
        this.hllCount = hllCount;
        this.valueLengthTotal = valueLengthTotal;
        this.valueLengthMax = valueLengthMax;
        this.valueLengthMin = valueLengthMin;
    }

    public long getCount() {
        return this.count;
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
        result += entry("propertyName", propertyName);
        result += ", ";
        result += entry("cms", count);
        result += ", ";
        result += entry("hll", hllCount);
        result += ", ";
        result += entry("valueLengthTotal", valueLengthTotal);
        result += ", ";
        result += entry("valueLengthMax", valueLengthMax);
        result += ", ";
        result += entry("valueLengthMin", valueLengthMin);
        result += "}";
        return result;
    }

    private String entry(String name, String val) {
        return "\"" + name + "\"" + " : " + "\"" + val + "\"";
    }

    private String entry(String name, long val) {
        return "\"" + name + "\"" + " : " + val;
    }

    public static void main(String[] args) {
        long avgLength = 400L;
        long maxLength = 30000L;
        long minLength = 200L;
        EstimationResult er = new EstimationResult("jcr:primaryType", 42, 1, avgLength, maxLength, minLength);
        System.out.println(er);
    }
//    {"propertyName" : "jcr:primaryType", "cms" : "42", "hll" : "1"}

}
