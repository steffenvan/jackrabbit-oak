package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import static org.junit.Assert.assertEquals;

public class TopKValuesTest {
    private TopKValues topValues;

    @Before
    public void before() {
        this.topValues = new TopKValues(new PriorityQueue<>(), 5, new HashSet<>());
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
//        List<TopKValues.ValueCountPair> TopKValues.ValueCountPairList = topValues.get();
//        assert(TopKValues.ValueCountPairList.get(4).getCount() == 1);
//        assert(TopKValues.ValueCountPairList.get(4).getName().equals("baz"));
//
//        topValues.update("xyz", 10);
//        assert(topValues.size() == 5);
//        TopKValues.ValueCountPairList = topValues.get();
//        assert(TopKValues.ValueCountPairList.get(4).getCount() == 2);
//        assert(TopKValues.ValueCountPairList.get(4).getName().equals("foobaz"));
    }


    @Test
    public void testUpdateWhileLoop() {
        for (int i = 0; i < 20; i++) {
            topValues.update("foo" + i, 100 - i);
        }

        assert(topValues.size() <= 5);
    }
    @Test
    public void testAddDuplicates() {
        topValues.update("foobaz", 100);
        topValues.update("foobaz", 123);

        assert(topValues.size() == 1);
    }

    @Test
    public void testToString() {
        topValues.update("foo", 10);
        topValues.update("bar", 20);
        topValues.update("foobar", 30);
        topValues.update("baz", 4);
        topValues.update("xyz", 50);
        topValues.update("abc", 50);
        String actual = topValues.toString();
        String expected = "{xyz : 50, abc : 50, foobar : 30, bar : 20, foo : 10}";
//        assertEquals(expected, actual);

        topValues.clear();
        topValues.update("function check() { return true; }", 11);
        topValues.update("function check() { if (workflowData.getMetaDataMap().get(\"lastTaskAction\",\"\") == \"Approve\") { return true } ... [-2016811243]", 7);
        topValues.update("var workflowInitiator = workItem.getWorkflow().getInitiator(); task.setCurrentAssignee(workflowInitiator); // set taskDueDate ... [588267296]", 5);
        actual = topValues.toString();
        String test = "[empty header.Referer ? granite:concat(\"/assets.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer:16, empty header.Referer ? granite:concat(\"/sites.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer:9, empty header.Referer ? granite:concat(\"/projects.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer:7, empty header.Referer ? granite:concat(\"/assets.html\", granite:encodeURIPath(empty param.item ? requestPathInfo.suffix : param. ... [-285298351]:6, empty header.Referer ? granite:encodeURIPath(\"/aem/start.html\") : header.Referer:6]";
        System.out.println(actual);
//        expected = "{\"function check() { return true; }\": 11, \"function check() { if (workflowData.getMetaDataMap().get(\\\"lastTaskAction\\\",\\\"\\\") == \\\"Approve\\\") { return true } ... [-2016811243]\": 7, var workflowInitiator = workItem.getWorkflow().getInitiator(); task.setCurrentAssignee(workflowInitiator); // set taskDueDate ... [588267296]}";
//        System.out.println(expected);
//        assertEquals(actual, expected);
    }

    @Test
    public void testGetTopValues() {
        String s = "function check() { return true; }";
        System.out.println(s);
        s = JsopBuilder.encode(s);
        System.out.println(s);
    }

    @Test
    public void testDeserialize() {
        TopKValues.ValueCountPair a = new TopKValues.ValueCountPair("foo", 3L);
        TopKValues.ValueCountPair b = new TopKValues.ValueCountPair("bar", 5L);
        TopKValues.ValueCountPair c = new TopKValues.ValueCountPair("foobar", 8L);
        TopKValues.ValueCountPair d = new TopKValues.ValueCountPair("baz", 1L);
        TopKValues.ValueCountPair e = new TopKValues.ValueCountPair("foobaz", 2L);
    }
    @Test
    public void testDifficult() {
        List<TopKValues.ValueCountPair> top = new ArrayList<>();
        TopKValues.ValueCountPair tpi = new TopKValues.ValueCountPair("empty header.Referer ? granite:concat(\"/assets.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer", 16L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair("empty header.Referer ? granite:concat(\"/sites.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer", 9L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair("empty header.Referer ? granite:concat(\"/projects.html\", granite:encodeURIPath(requestPathInfo.suffix)) : header.Referer", 7L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair("empty header.Referer ? granite:concat(\"/assets.html\", granite:encodeURIPath(empty param.item ? requestPathInfo.suffix : param. ... [-285298351]", 6L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair("empty header.Referer ? granite:encodeURIPath(\"/aem/start.html\") : header.Referer", 6L);
        top.add(tpi);

        String out = "{" + JsopBuilder.encode(top.toString()) + "}";
        System.out.println(out);
//        TopKPropertyInfo
    }

}
