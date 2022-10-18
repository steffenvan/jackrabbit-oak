package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.cache.Cache;

import java.util.*;

public class TopKElements {
	private final int k;
	private PriorityQueue<ValueCountPair> topValues;
	private HashSet<String> currValues;

	public static class ValueCountPair implements Comparable<ValueCountPair> {
		String value;
		long count;

		public ValueCountPair(String name, Long count) {
			this.value = name;
			this.count = count;
		}

		@Override
		public int compareTo(ValueCountPair o) {
			return Long.compare(count, o.count);
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + value.hashCode();
			result = (int) (prime * result + count);
			return result;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null) {
				return false;
			}
			if (this.getClass() != o.getClass()) {
				return false;
			}
			ValueCountPair other = (ValueCountPair) o;
			return this.value.equals(other.value);
		}
	}

	public TopKElements(PriorityQueue<ValueCountPair> topValues, int k, HashSet<String> currValues) {
		this.topValues = topValues;
		this.k = k;
		this.currValues = currValues;
	}

	public void clear() {
		this.topValues.clear();
	}

	private boolean replaceLowest
	void update(Object val, long count) {
		String name = val.toString();
		ValueCountPair curr = new ValueCountPair(name, count);
		if (currValues.contains(name)) {
			topValues.remove(curr);
			currValues.remove(name);
		}

		if (topValues.size() >= k) {
			ValueCountPair top = topValues.peek();
			assert top != null;
			if (count >= top.count) {
				topValues.poll();
//				topValues.offer(curr);
			}
		}
		topValues.offer(curr);
		currValues.add(name);
	}

	public List<PropertyInfo> get() {
		List<PropertyInfo> propertyInfos = new ArrayList<>();
		topValues.forEach(x -> propertyInfos.add(new PropertyInfo(x.value, x.count)));
		propertyInfos.sort(Comparator.comparing(PropertyInfo::getCount).reversed());

		// the number of property values can sometimes be less than k
		int topElementIdx = Math.min(propertyInfos.size(), k);

		return propertyInfos.subList(0, topElementIdx);
	}

	public int size() {
		return this.topValues.size();
	}

	public static PriorityQueue<ValueCountPair> deserialize(Iterable<String> names, Iterable<Long> counts, int k) {
		Iterator<String> namesIter = names.iterator();
		Iterator<Long> countsIter = counts.iterator();
		PriorityQueue<ValueCountPair> topValues = new PriorityQueue<>();

		while (namesIter.hasNext() && countsIter.hasNext()) {
			String name = namesIter.next();
			Long count = countsIter.next();
			if (topValues.size() >= k) {
				ValueCountPair top = topValues.peek();
				assert top != null;
				if (count >= top.count) {
					topValues.poll();
					topValues.offer(new ValueCountPair(name, count));
				}
			} else {
				topValues.offer(new ValueCountPair(name, count));
			}
		}
		return topValues;
	}

	public Set<String> getNames() {
		Set<String> names = new HashSet<>();
		for (ValueCountPair topValue : topValues) {
			names.add(topValue.value);
		}
		return names;
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
