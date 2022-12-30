package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

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
        TopKValues topKValues = new TopKValues(k);

        while (valuesIter.hasNext() && countsIter.hasNext()) {
            String value = valuesIter.next();
            Long count = countsIter.next();
            updateValue(topKValues, k, new ValueCountPair(value, count));
        }

        return topKValues;
    }

    public static TopKValues fromPropertyStates(PropertyState valueNames,
                                                PropertyState valueCounts,
                                                int k) {

        if (valueNames != null && valueCounts != null) {
            @NotNull Iterable<String> valueNamesIter = valueNames.getValue(
                    Type.STRINGS);
            @NotNull Iterable<Long> valueCountsIter = valueCounts.getValue(
                    Type.LONGS);

            return TopKValues.createFromIndex(valueNamesIter, valueCountsIter,
                                              k);
        }

        return new TopKValues(k);
    }

    /**
     * Inserts (and perhaps updates) the TopKValues object with a new value and
     * its associated count. If the value we want to insert already exists in
     * the object, it will very likely have an outdated count which is why we
     * need to remove and re-insert it.
     *
     * @param t    the topKValues object that is updated
     * @param k    size of k
     * @param curr the ValueCountPair that we want to insert into the object
     */
    private static void updateValue(TopKValues t, int k, ValueCountPair curr) {

        if (t.contains(curr.getValue())) {
            t.remove(curr);
        }

        if (t.size() >= k) {
            ValueCountPair top = t.peek();
            assert top != null;
            if (curr.getCount() >= top.getCount()) {
                t.remove(curr);
            }
        }

        if (t.size() < k) {
            t.insert(curr);
        }
    }

    void update(String value, long count) {
        updateValue(this, k, new ValueCountPair(value, count));
    }

    public boolean contains(String value) {
        return currValues.contains(value);
    }

    private void remove(ValueCountPair vcp) {
        topValues.remove(vcp);
        currValues.remove(vcp.getValue());
    }

    private void insert(ValueCountPair vcp) {
        topValues.offer(vcp);
        currValues.add(vcp.getValue());
    }

    /**
     * Retrieves the head of the priority queue if it's non-empty. Otherwise, it
     * returns null as done in Java's STL.
     *
     * @return the head of the priority queue or null if it's empty
     */
    private ValueCountPair peek() {
        return topValues.peek();
    }

    public boolean isEmpty() {
        return currValues.isEmpty() && topValues.isEmpty();
    }

    void clear() {
        topValues.clear();
        currValues.clear();
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
            builder.value(getCount());

            builder.key("totalCount");
            builder.value(totalValueCount);

            builder.key("percentage");
            builder.value(percentage());

            builder.endObject();

            return builder.toString();
        }
    }

    public static class ValueCountPair implements Comparable<ValueCountPair> {
        private final String value;
        private final long count;

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
