package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

public class TopKElementsTest {
    private TopKElements topValues;

    @Before
    public void before() {
        this.topValues = new TopKElements(new PriorityQueue<>(), 5, new HashSet<>());
    }

    @After
    public void after() {
        this.topValues.clear();
    }

    @Test
    public void testAdd() {
        topValues.update("foo", 3);
        topValues.update("bar", 5);
        topValues.update("foobar", 8);
        topValues.update("baz", 1);
        topValues.update("foobaz", 2);
        assert(topValues.size() == 5);

        // getting the top values in descending order.
        List<PropertyInfo> propertyInfoList = topValues.get();
        assert(propertyInfoList.get(4).getCount() == 1);
        assert(propertyInfoList.get(4).getName().equals("baz"));

        topValues.update("xyz", 10);
        assert(topValues.size() == 5);
        propertyInfoList = topValues.get();
        assert(propertyInfoList.get(4).getCount() == 2);
        assert(propertyInfoList.get(4).getName().equals("foobaz"));
    }

    @Test
    public void testAddDuplicates() {
        topValues.update("foobaz", 100);
        topValues.update("foobaz", 123);

        assert(topValues.size() == 1);
    }

    @Test
    public void testGetTopValues() {

    }

    @Test
    public void testDeserialize() {
        TopKElements.ValueCountPair a = new TopKElements.ValueCountPair("foo", 3L);
        TopKElements.ValueCountPair b = new TopKElements.ValueCountPair("bar", 5L);
        TopKElements.ValueCountPair c = new TopKElements.ValueCountPair("foobar", 8L);
        TopKElements.ValueCountPair d = new TopKElements.ValueCountPair("baz", 1L);
        TopKElements.ValueCountPair e = new TopKElements.ValueCountPair("foobaz", 2L);
    }

}
