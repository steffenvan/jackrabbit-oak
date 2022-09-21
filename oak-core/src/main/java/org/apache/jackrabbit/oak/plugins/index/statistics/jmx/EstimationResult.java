package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

public class EstimationResult {
    private final String propertyName;
    private final long count;
    private final long hllCount;

    EstimationResult(String propertyName, long count, long hllCount) {
        this.propertyName = propertyName;
        this.count = count;
        this.hllCount = hllCount;
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
        result += "\"" + "propertyName" + "\"" + " : " + "\"" + propertyName + "\"";
        result += ", ";
        result += "\"" + "count" + "\"" + " : " + "\"" + count + "\"";
        result += ", ";
        result += "\"" + "unique" + "\"" + " : " + "\"" + hllCount + "\"";
        result += "}";
        return result;
    }

    public static void main(String[] args) {
        EstimationResult er = new EstimationResult("jcr:primaryType", 42, 1);
        System.out.println(er);
    }
}
