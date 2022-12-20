package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;

import java.util.*;

/**
 * Contains and implements the logic to maintain the top-K values of a property.
 * Internally, it uses a priority queue that is updated each time a value is
 * added or modified. In fact, whenever an existing value is updated, we check
 * if it should be added to the priority queue.
 */
public class TopKValues {
    private final int k;
    private final PriorityQueue<ValueCountPair> topValues;
    private final Set<String> currValues;

    private TopKValues(PriorityQueue<ValueCountPair> topValues, int k,
                       Set<String> currValues) {
        this.topValues = topValues;
        this.k = k;
        this.currValues = currValues;
    }

    public TopKValues(int count) {
        this(new PriorityQueue<>(), count, new HashSet<>());
    }

    /**
     * Static factory for reading and creating the top K values object for a
     * property that is already stored at the statistics index.
     *
     * @param values the top K values
     * @param counts the top K counts
     * @param k      the actual top K number
     * @return the top K values object
     */
    public static TopKValues createFromIndex(Iterable<String> values,
                                             Iterable<Long> counts, int k) {
        Iterator<String> valuesIter = values.iterator();
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

        return new TopKValues(topKValues, k, currValues);
    }

    public PriorityQueue<ValueCountPair> getTopK() {
        return new PriorityQueue<>(topValues);
    }

    public void clear() {
        this.topValues.clear();
    }

    void update(String value, long count) {
        ValueCountPair curr = new ValueCountPair(value, count);

        // if the current value is already in the priority queue it will have
        // an outdated count, which is why we remove it and re-insert it
        if (currValues.contains(value)) {
            currValues.remove(value);
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

    /**
     * Gets the top k values for a property. The information is returned as a
     * ValueCountPair
     * TODO: This function currently pops the whole priority queue. This
     * should be optimized such that we only do it for min(k, pq.size()).
     *
     * @return the top k values
     */
    public List<ValueCountPair> get() {
        List<ValueCountPair> valueCountPairs = new ArrayList<>();
        topValues.forEach(
                x -> valueCountPairs.add(new ValueCountPair(x.value, x.count)));
        valueCountPairs.sort(
                Comparator.comparing(ValueCountPair::getCount).reversed());

        // the number of values can sometimes be less than k. E.g. boolean
        // values
        int topElementIdx = Math.min(valueCountPairs.size(), k);

        return valueCountPairs.subList(0, topElementIdx);
    }

    public int size() {
        return this.topValues.size();
    }

    public Set<String> getValues() {
        Set<String> names = new HashSet<>();
        for (ValueCountPair topValue : topValues) {
            names.add(topValue.value);
        }

        return names;
    }

    @Override
    public String toString() {
        List<ValueCountPair> topKSortedValues = get();
        JsopBuilder builder = new JsopBuilder();
        builder.object();

        for (ValueCountPair vcp : topKSortedValues) {
            builder.key(vcp.getValue());
            builder.value(vcp.getCount());
        }

        builder.endObject();

        return builder.toString();
    }

    public static class ProportionInfo {
        private final String value;
        private final long count;
        private final long totalValueCount;

        public ProportionInfo(String value, long count, long totalValueCount) {
            this.value = value;
            this.count = count;
            this.totalValueCount = totalValueCount;
        }

        String percentage() {
            String percent = String.valueOf(
                    Math.round(((double) count / totalValueCount) * 100));
            return percent + "%";
        }

        public long getCount() {
            return count;
        }

        @Override
        public String toString() {
            JsopBuilder builder = new JsopBuilder();
            builder.object();

            builder.key("value");
            builder.value(value);

            builder.key("count");
            builder.value(count);

            builder.key("totalCount");
            builder.value(totalValueCount);

            builder.key("percentage");
            builder.value(percentage());

            builder.endObject();

            return builder.toString();
        }
    }

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
    }
}
