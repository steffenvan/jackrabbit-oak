package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.statistics.TopKElements;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKElements.ValueCountPair;

public class DetailedPropertyInfo {

	private List<TopKElements.ValueCountPair> sortedTopK;
	private List<TopKElements.ValueCountPair> allProperties;
	private String name;
	private long count;
	private long totalCount;
	private double percentage;

	DetailedPropertyInfo(List<TopKElements.ValueCountPair> stk, List<TopKElements.ValueCountPair> allProps) {
		this.sortedTopK = stk;
		this.allProperties = allProps;
	}

	DetailedPropertyInfo(String name, long count, long totalCount) {
		this.name = name;
		this.count = count;
		this.totalCount = totalCount;
		this.percentage = percentage();

	}

	TopKElements.ValueCountPair max() {
		return Collections.max(allProperties, Comparator.comparing(TopKElements.ValueCountPair::getCount));
	}

	TopKElements.ValueCountPair min() {
		return Collections.min(allProperties, Comparator.comparing(TopKElements.ValueCountPair::getCount));
	}

	double percentage() {
		return Math.round(((double) count / totalCount) * 100);
	}

	long getCount() {
		return count;
	}

	@Override
	public String toString() {
		return name + " | " + count + "/" + totalCount + " | " + percentage + "%";
	}
}
