package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class TopKElements {
	Iterable<Long> counts;
	String[] values;
	Map<String, Long> valuesToCounts;

	TopKElements(String[] topValues, Iterable<Long> topCounts, long numCounts) {
		this.counts = topCounts;
		this.values = topValues;
//		this.elements = createMinHeap(topValues, topCounts, numCounts);
	}

	TopKElements(Map<String, Long> valuesToCounts) {
		this.valuesToCounts = valuesToCounts;
	}

	List<PropertyInfo> getAllSorted() {
		List<PropertyInfo> elements = new ArrayList<>();
//		PriorityQueue/<PropertyInfo> elements = new PriorityQueue<>(Comparator.comparingLong(PropertyInfo::getCount));
		for (Entry<String, Long> e : valuesToCounts.entrySet()) {
			PropertyInfo propInfo = new PropertyInfo(e.getKey(), e.getValue());
			elements.add(propInfo);
		}
		elements.sort(Comparator.comparing(PropertyInfo::getCount));

		return elements;
	}

//	public boolean contains(String name) {
//		return Arrays.stream(values).anyMatch(name::equals);
//	}

	boolean isEmpty() {
		return valuesToCounts.isEmpty();
	}

	void update(String name, long count) {
		valuesToCounts.computeIfPresent(name, (k, v) -> v + count);
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

	public void add(String name, long count) {
		valuesToCounts.put(name, count);
	}

	public long minElement() {
		return Collections.min(valuesToCounts.values());
	}

	public String serialize() {
		StringBuilder sb = new StringBuilder();
		List<PropertyInfo> sortedElements = getAllSorted();
		for (PropertyInfo pi : sortedElements) {
			sb.append(pi.toString());
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
