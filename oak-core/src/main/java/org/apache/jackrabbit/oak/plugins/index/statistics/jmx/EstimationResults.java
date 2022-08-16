package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

class EstimationResults {
	private final long hllCount;
	private final long cmsCount;

	EstimationResults(long hllCount, long cmsCount) {
		this.hllCount = hllCount;
		this.cmsCount = cmsCount;
	}

	@Override
	public String toString() {
		return "hllCount: " + this.hllCount + "cmsCount: " + this.cmsCount;
	}
}
