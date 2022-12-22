package org.apache.jackrabbit.oak.plugins.index.statistics;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TopKValuesTest {
    private TopKValues topValues;

    @Before
    public void before() {
        this.topValues = new TopKValues(5);
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
        assert (topValues.size() == 5);
    }


    @Test
    public void testUpdateWhileLoop() {
        for (int i = 0; i < 20; i++) {
            topValues.update("foo" + i, 100 - i);
        }

        assert (topValues.size() <= 5);
    }

    @Test
    public void testAddDuplicates() {
        topValues.update("foobaz", 100);
        topValues.update("foobaz", 123);

        assert (topValues.size() == 1);
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
        String expected = "{xyz : 50, abc : 50, foobar : 30, bar : 20, foo : "
                + "10}";
        //        assertEquals(expected, actual);

        topValues.clear();
        topValues.update("function check() { return true; }", 11);
        topValues.update(
                "function check() { if (workflowData.getMetaDataMap().get" +
                        "(\"lastTaskAction\",\"\") == \"Approve\") { return " + "true } ... [-2016811243]",
                7);
        topValues.update(
                "var workflowInitiator = workItem.getWorkflow().getInitiator" + "(); task.setCurrentAssignee(workflowInitiator); // " + "set taskDueDate ... [588267296]",
                5);
        actual = topValues.toString();
        String test = "[empty header.Referer ? granite:concat(\"/assets" +
                ".html\", granite:encodeURIPath(requestPathInfo.suffix)) : " + "header.Referer:16, empty header.Referer ? granite:concat" + "(\"/sites.html\", granite:encodeURIPath(requestPathInfo" + ".suffix)) : header.Referer:9, empty header.Referer ? " + "granite:concat(\"/projects.html\", granite:encodeURIPath" + "(requestPathInfo.suffix)) : header.Referer:7, empty header" + ".Referer ? granite:concat(\"/assets.html\", " + "granite:encodeURIPath(empty param.item ? requestPathInfo" + ".suffix : param. ... [-285298351]:6, empty header.Referer ? " + "granite:encodeURIPath(\"/aem/start.html\") : header" + ".Referer:6]";
        System.out.println(actual);
    }

    @Test
    public void testGetTopValues() {
        Map<String, Long> m = ImmutableMap.of("foo", 3L, "bar", 500L, "foobar",
                                              80L, "baz", 1L, "foobaz", 2L);

        TopKValues t = TopKValues.createFromIndex(m.keySet(), m.values(), 3);
        List<TopKValues.ValueCountPair> sorted = t.get();
        assertEquals(sorted.get(0), new TopKValues.ValueCountPair("bar", 300L));
        assertEquals(sorted.get(1),
                     new TopKValues.ValueCountPair("foobar", 80L));
        assertEquals(sorted.get(2), new TopKValues.ValueCountPair("foo", 3L));
        assertEquals(t.getValues(), Sets.newHashSet("bar", "foobar", "foo"));
    }

    @Test
    public void testCreateFromIndex() {
        List<String> values = Lists.newArrayList("foo", "bar", "foobar", "baz",
                                                 "foobaz");
        List<Long> counts = Lists.newArrayList(3L, 5L, 8L, 1L, 2L);

        TopKValues topValues = TopKValues.createFromIndex(values, counts, 5);
        assertTrue(topValues.contains("foo"));
        assertTrue(topValues.contains("bar"));
        assertTrue(topValues.contains("foobar"));
        assertTrue(topValues.contains("baz"));
        assertTrue(topValues.contains("foobaz"));
    }

    @Test
    public void testDifficult() {
        List<TopKValues.ValueCountPair> top = new ArrayList<>();
        TopKValues.ValueCountPair tpi = new TopKValues.ValueCountPair(
                "empty header.Referer ? granite:concat(\"/assets.html\", " +
                        "granite:encodeURIPath(requestPathInfo.suffix)) : " + "header.Referer",
                16L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair(
                "empty header.Referer ? granite:concat(\"/sites.html\", " +
                        "granite:encodeURIPath(requestPathInfo.suffix)) : " + "header.Referer",
                9L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair(
                "empty header.Referer ? granite:concat(\"/projects.html\", " + "granite:encodeURIPath(requestPathInfo.suffix)) : " + "header.Referer",
                7L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair(
                "empty header.Referer ? granite:concat(\"/assets.html\", " +
                        "granite:encodeURIPath(empty param.item ? " +
                        "requestPathInfo.suffix : param. ... [-285298351]",
                6L);
        top.add(tpi);

        tpi = new TopKValues.ValueCountPair(
                "empty header.Referer ? granite:encodeURIPath(\"/aem/start" + ".html\") : header.Referer",
                6L);
        top.add(tpi);


        String out = JsopBuilder.encode(top.toString());
        System.out.println(out);
        //        TopKPropertyInfo
    }

    @Test
    public void testValueCountPairEquals() {
        TopKValues.ValueCountPair vcp1 = new TopKValues.ValueCountPair("foo",
                                                                       30L);
        TopKValues.ValueCountPair vcp2 = new TopKValues.ValueCountPair("bar",
                                                                       15L);
        TopKValues.ValueCountPair vcp3 = new TopKValues.ValueCountPair("foobar",
                                                                       42L);

        TopKValues.ValueCountPair vcp4 = new TopKValues.ValueCountPair("foo",
                                                                       23L);

        assertEquals(vcp1, vcp1);
        assertNotEquals(vcp1, null);
        assertNotEquals(vcp1, vcp2);
        assertNotEquals(vcp1, vcp3);
        assertNotEquals(vcp2, vcp3);
        assertEquals(vcp1, vcp4);
        assertNotEquals(vcp1, new TopKValues.ProportionInfo("foo", 30L, 30L));

    }

    @Test
    public void testProportionalInfoToString() {
        long total = 100;
        TopKValues.ProportionInfo pi1 = new TopKValues.ProportionInfo("foo",
                                                                      30L,
                                                                      total);
        TopKValues.ProportionInfo pi2 = new TopKValues.ProportionInfo("bar",
                                                                      60L,
                                                                      total);
        TopKValues.ProportionInfo pi3 = new TopKValues.ProportionInfo("foobar",
                                                                      10L,
                                                                      total);

        String expected_1 = "{\"value\":\"foo\",\"count\":30," +
                "\"totalCount\":100,\"percentage\":\"30%\"}";

        String expected_2 = "{\"value\":\"bar\",\"count\":60," +
                "\"totalCount\":100,\"percentage\":\"60%\"}";

        assertEquals(pi1.toString(), expected_1);
        assertEquals(pi2.toString(), expected_2);
        assertNotEquals(pi1.toString(), expected_2);
    }

}
