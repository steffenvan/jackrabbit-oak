package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

import java.util.*;

public class TopKElements {
	private final int k;
	private final PriorityQueue<ValueCountPair> topValues;
	private final Set<String> currValues;

	public static class ValueCountPair implements Comparable<ValueCountPair> {
		String value;
		long count;

		public ValueCountPair(String value, Long count) {
			this.value = value;
			this.count = count;
		}

		@Override
		public int compareTo(ValueCountPair o) {
			return Long.compare(count, o.count);
		}

		@Override
		public int hashCode() {
		    return value.hashCode();
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

		private String entry(String name, long val) {
			name = JsopBuilder.encode(name);
			return name + " : " + val;
		}
		@Override
		public String toString() {
			return entry(value, count);
		}
	}

	public TopKElements(PriorityQueue<ValueCountPair> topValues, int k, Set<String> currValues) {
		this.topValues = topValues;
		this.k = k;
		this.currValues = currValues;
	}

	public PriorityQueue<ValueCountPair> getTopK() {
		return new PriorityQueue<>(topValues);
	}

	public void clear() {
		this.topValues.clear();
	}

	void update(Object val, long count) {
		String value = val.toString();
		ValueCountPair curr = new ValueCountPair(value, count);
		if (currValues.contains(value)) {
			currValues.remove(value);
			// if the current value is already in the priority queue it will have an outdated count, which is why we
			// remove it and re-insert it with the   d count
			topValues.remove(curr);
		}

		if (topValues.size() >= k) {
			ValueCountPair top = topValues.peek();
			assert top != null;
			if (count >= top.count) {
				ValueCountPair old = topValues.poll();
				currValues.remove(old.value);
			}
		}

		if (topValues.size() < k) {
			topValues.offer(curr);
			currValues.add(value);
		}
	}

	public List<TopKPropertyInfo> get() {
		List<TopKPropertyInfo> topKPropertyInfos = new ArrayList<>();
		topValues.forEach(x -> topKPropertyInfos.add(new TopKPropertyInfo(x.value, x.count)));
		topKPropertyInfos.sort(Comparator.comparing(TopKPropertyInfo::getCount).reversed());

		// the number of property values can sometimes be less than k
		int topElementIdx = Math.min(topKPropertyInfos.size(), k);

		return topKPropertyInfos.subList(0, topElementIdx);
	}

	public int size() {
		return this.topValues.size();
	}

	public static PriorityQueue<ValueCountPair> deserialize(Iterable<String> names, Iterable<Long> counts, int k) {
		Iterator<String> valuesIter = names.iterator();
		Iterator<Long> countsIter = counts.iterator();
		Set<String> currValues = new HashSet<>();
		PriorityQueue<ValueCountPair> topKValues = new PriorityQueue<>();

		while (valuesIter.hasNext() && countsIter.hasNext()) {
			String value = valuesIter.next();
			Long count = countsIter.next();
			ValueCountPair curr = new ValueCountPair(value, count);
			if (currValues.contains(value)) {
				currValues.remove(value);
				topKValues.remove(curr);
			}

			if (topKValues.size() >= k) {
				ValueCountPair top = topKValues.peek();
				assert top != null;
				if (count >= top.count) {
					ValueCountPair old = topKValues.poll();
					currValues.remove(old.value);
				}
			}

			if (topKValues.size() < k) {
				topKValues.offer(curr);
				currValues.add(value);
			}
		}

		return topKValues;
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
		List<TopKPropertyInfo> topKSortedValues = get();
		return JsopBuilder.encode(topKSortedValues.toString());
	}
}
