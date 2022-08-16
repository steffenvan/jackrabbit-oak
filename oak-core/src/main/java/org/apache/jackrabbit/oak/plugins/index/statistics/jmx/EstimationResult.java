package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

class EstimationResult {
	private final long hllCount;
	private final long count;

	EstimationResult(long hllCount, long count) {
		this.hllCount = hllCount;
		this.count = count;
	}

	@Override
	public String toString() {
		return "hllCount: " + this.hllCount + "count: " + this.count;
	}
}
