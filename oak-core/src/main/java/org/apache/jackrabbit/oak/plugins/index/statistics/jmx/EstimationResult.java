package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

class EstimationResult {
    private final String propertyName;
	private final long count;
	private final long hllCount;

	EstimationResult(String propertyName, long count, long hllCount) {
	    this.propertyName = propertyName;
		this.count = count;
		this.hllCount = hllCount;
	}

	@Override
	public String toString() {
		return "{ propertyName: \"" + propertyName + "\"," +
		        " count: " + this.count + ", " +
		        " distinct: " + this.hllCount +
		        "}";
	}
}
