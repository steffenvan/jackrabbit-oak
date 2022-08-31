package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TopKElements {
	private Map<String, Long> valuesToCounts;
	private final int k;

	public TopKElements(Map<String, Long> valuesToCounts, int k) {
		this.valuesToCounts = valuesToCounts;
		this.k = k;
	}

	void update(Object val, long count) {
		valuesToCounts.put(val.toString(), count);
	}

	public List<PropertyInfo> get() {
		List<PropertyInfo> propertyInfos = new ArrayList<>();
		for (Map.Entry<String, Long> e : valuesToCounts.entrySet()) {
			propertyInfos.add(new PropertyInfo(e.getKey(), e.getValue()));
		}

		propertyInfos.sort(Comparator.comparing(PropertyInfo::getCount).reversed());
		// the number of property values can sometimes be less than k
		int topElementIdx = Math.min(propertyInfos.size(), k);

		return propertyInfos.subList(0, topElementIdx);
	}

	public static Map<String, Long> deserialize(Iterable<String> names, Iterable<Long> counts) {
		Map<String, Long> valuesToCounts = new HashMap<>();
		Iterator<String> namesIter = names.iterator();
		Iterator<Long> countsIter = counts.iterator();

		while (namesIter.hasNext() && countsIter.hasNext()) {
			valuesToCounts.put(namesIter.next(), countsIter.next());
		}

		return valuesToCounts;
	}

	@Override
	public String toString() {
		List<PropertyInfo> topKSortedValues = get();
		StringBuilder sb = new StringBuilder();

		for (PropertyInfo pi : topKSortedValues) {
			sb.append(pi.toString());
			sb.append(" ");
		}

		return sb.toString();
	}
}
