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
        String result = "{" + "\"" + propertyName + "\"" + " : ";
        result += "{" + "\"" + "count" + "\"" + " : " + "\"" + count + "\"" + ", ";
        result += "\"" + "unique" + "\"" + " : " + "\"" + hllCount + "\"";
        result += "}";
        result += "}";
        return result;
    }
}
