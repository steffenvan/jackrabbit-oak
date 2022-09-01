package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyInfo;

public class DetailedPropertyInfo {

	private List<PropertyInfo> sortedTopK;
	private List<PropertyInfo> allProperties;
	private String name;
	private long count;
	private long totalCount;
	private double percentage;

	DetailedPropertyInfo(List<PropertyInfo> stk, List<PropertyInfo> allProps) {
		this.sortedTopK = stk;
		this.allProperties = allProps;
	}

	DetailedPropertyInfo(String name, long count, long totalCount) {
		this.name = name;
		this.count = count;
		this.totalCount = totalCount;
		this.percentage = percentage();

	}

	PropertyInfo max() {
		return Collections.max(allProperties, Comparator.comparing(PropertyInfo::getCount));
	}

	PropertyInfo min() {
		return Collections.min(allProperties, Comparator.comparing(PropertyInfo::getCount));
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
