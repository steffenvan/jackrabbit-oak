package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TopKElements {
	private Map<String, Long> valuesToCounts;
	private final int k;

	TopKElements(Map<String, Long> valuesToCounts, int k) {
		this.valuesToCounts = valuesToCounts;
		this.k = k;
	}

	private List<PropertyInfo> getAllSorted() {
		List<PropertyInfo> propertyInfos = new ArrayList<>();
		for (Entry<String, Long> e : valuesToCounts.entrySet()) {
			PropertyInfo pi = new PropertyInfo(e.getKey(), e.getValue());
			propertyInfos.add(pi);
		}

		propertyInfos.sort(Comparator.comparing(PropertyInfo::getCount).reversed());
		int idx = Math.min(propertyInfos.size(), k);
		return propertyInfos.subList(0, idx);
	}

	boolean isEmpty() {
		return valuesToCounts.isEmpty();
	}

	void update(Object val, long count) {
		valuesToCounts.put(val.toString(), count);
	}

	public boolean contains(String name) {
		return this.valuesToCounts.containsKey(name);
	}

	private String getMinValue() {
		String minKey = null;
		long minCount = Long.MAX_VALUE;
		for (Entry<String, Long> entry : valuesToCounts.entrySet()) {
			long count = entry.getValue();
			if (count < minCount) {
				minCount = count;
				minKey = entry.getKey();
			}
		}
		return minKey;
	}

	public void removeMinElement() {
		String minElementName = getMinValue();
		valuesToCounts.remove(minElementName);
	}

	public long minElement() {
		return Collections.min(valuesToCounts.values());
	}

	public void clear() {
		valuesToCounts.clear();
	}

	public String serialize() {
		List<PropertyInfo> sortedElements = getAllSorted();
		StringBuilder sb = new StringBuilder();

		if (sortedElements.isEmpty()) {
			return "";
		}

		int idx = Math.min(sortedElements.size(), k);

		for (int i = 0; i < idx; i++) {
			String el = sortedElements.get(i).toString();
			sb.append(el);
			sb.append(" ");
		}

		return sb.toString();
	}

	public static Map<String, Long> deserialize(String elements) {
		Map<String, Long> valuesToCounts = new HashMap<>();
		String[] parts = elements.split("\\s+");
		for (String s : parts) {
			String[] smallerParts = s.split("->");
			String name = smallerParts[0];
			Long count = Long.parseLong(smallerParts[1]);
			valuesToCounts.put(name, count);
		}

		return valuesToCounts;

	}
}
