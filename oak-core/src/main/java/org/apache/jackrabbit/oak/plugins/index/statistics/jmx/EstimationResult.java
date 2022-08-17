package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

class EstimationResult {
	private final long count;
	private final long hllCount;

	EstimationResult(long count, long hllCount) {
		this.count = count;
		this.hllCount = hllCount;
	}

	@Override
	public String toString() {
		return "count: " + this.count + " | " + "hllCount: " + this.hllCount;
	}
}
