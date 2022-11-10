package org.apache.jackrabbit.oak.plugins.index.statistics;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;
import java.util.Comparator;

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

		public Long getCount() {
			return count;
		}

		public String getValue() {
			return value;
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

		@Override
		public String toString() {
			try {
				return new ObjectMapper().writeValueAsString(this);
			} catch (JsonProcessingException e) {
				throw new RuntimeException(e);
			}
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

	// TODO: This function currently pops the whole priority queue. This should
	// be optimized such that we only do it for min(k, pq.size()).
	public List<ValueCountPair> get() {
		List<ValueCountPair> valueCountPairs = new ArrayList<>();
		topValues.forEach(x -> valueCountPairs.add(new ValueCountPair(x.value, x.count)));
		valueCountPairs.sort(Comparator.comparing(ValueCountPair::getCount).reversed());

		// the number of property values can sometimes be less than k
		int topElementIdx = Math.min(valueCountPairs.size(), k);

		return valueCountPairs.subList(0, topElementIdx);
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

//	@Override
//	public String toString() {
//		List<ValueCountPair> topKSortedValues = get();
//		return topKSortedValues.toString();
//		ObjectMapper mapper = new ObjectMapper();
//		try {
//			return mapper.writeValueAsString(topKSortedValues);
//		} catch (JsonProcessingException e) {
//			throw new RuntimeException(e);
//		}
	}
//		StringBuilder sb = new StringBuilder();
//		for (TopKPropertyInfo tk : topKSortedValues) {
//			sb.append("{");
//			sb.append(JsopBuilder.encode(tk.toString()));
//			sb.append("}");
//		}
//		return sb.toString();
//		return JsopBuilder.encode(topKSortedValues.toString());
//	}
//		StringBuilder sb = new StringBuilder();
//		sb.append("{");
//		int n = topKSortedValues.size();
//		int size = Math.min(n, k);
//		for (int i = 0; i < size; i++) {
//			if (i < size - 1) {
//				sb.append(topKSortedValues.get(i).toString());
//				sb.append(", ");
//			} else {
//				sb.append(topKSortedValues.get(i).toString());
//			}
//		}
//		sb.append("}");
//		return JsopBuilder.encode(sb.toString());

//}