package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.Optional;

import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getIndexRoot;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyStatisticsTest {
    PropertyStatistics ps;
    private NodeStore nodeStore;
    private TestUtility utility;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException,
            CommitFailedException {
        String name = "foo";
        int m = 64;
        HyperLogLog hll = new HyperLogLog(64);
        CountMinSketch cms = new CountMinSketch(5, 16);
        TopKValues topValues = new TopKValues(5);
        ps = new PropertyStatistics(name, 5L, hll, cms, topValues);
        nodeStore = new MemoryNodeStore();
        utility = new TestUtility(nodeStore, "statistics");
    }

    @After
    public void after() {
        ps = null;
    }

    @Test
    public void fromPropertyNode() throws CommitFailedException {
        utility.addNodes();
        // just choosing an arbitrary property. Any valid one would suffice
        String propName = "jcr:isAbstract";
        NodeBuilder builder = TestUtility.getPropertyFromBuilder(nodeStore);

        Optional<PropertyStatistics> prop = PropertyStatistics.fromPropertyNode(
                propName, builder.getNodeState());

        if (prop.isEmpty()) {
            throw new RuntimeException("Something went wrong in the test");
        }

        PropertyStatistics p = prop.get();
        assertEquals(propName, p.getName());
        assertTrue(p.getCount() > 0);
        assertTrue(p.getCmsCount("true") > 0);
        assertTrue(p.getUniqueCount() > 0);
        assertTrue(p.getValueLengthTotal() > 0);
        assertTrue(p.getValueLengthMax() > 0);
        assertTrue(p.getValueLengthMin() < Long.MAX_VALUE);
        assertTrue(p.getTopKValuesDescending().size() <= p.getUniqueCount());
    }

    @Test
    public void testFromPropertyNode() throws CommitFailedException {
        // add some nodes so properties are persisted
        utility.addNodes();

        // oak:index/statistics/index/properties
        NodeState dataNode = StatisticsIndexHelper.getNodeFromIndexRoot(
                getIndexRoot(nodeStore));
        Optional<PropertyStatistics> ps = PropertyStatistics.fromPropertyNode(
                "jcr:isAbstract", dataNode);

        assertTrue(ps.isPresent());
    }

    @Test
    public void testToStringIsValidJson() throws NoSuchWorkspaceException,
            LoginException, CommitFailedException, ParseException {
        NodeStore nodeStore = new MemoryNodeStore();
        TestUtility utility = new TestUtility(nodeStore, "statistics");
        utility.addNodes();


        // just choosing an arbitrary property. Any valid one would suffice
        String propName = "jcr:isAbstract";
        NodeBuilder builder = TestUtility.getPropertyFromBuilder(nodeStore);

        Optional<PropertyStatistics> prop = PropertyStatistics.fromPropertyNode(
                propName, builder.getNodeState());

        // if toString was invalid JSON an exception would be thrown
        prop.ifPresent(p -> {
            try {
                JSONObject o = (JSONObject) JSONValue.parseWithException(
                        p.toString());
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        });

    }

    @Test
    public void testUpdateValues() {
        String value = "foo";
        for (int i = 0; i < 100; i++) {
            if (i % 5 == 0) {
                String val = "bar";
                ps.updateValueCount(val, Hash.hash64(val.hashCode()));
            }
            ps.updateValueCount(value, Hash.hash64(value.hashCode()));
        }

        long cmsCount = ps.getCmsCount("bar");
        assertTrue(cmsCount >= 10);
    }

    @Test
    public void testUpdateValueLength() {
        // empty property statistics
        assertEquals(0, ps.getValueLengthTotal());
        assertEquals(0, ps.getValueLengthMax());
        assertEquals(Long.MAX_VALUE, ps.getValueLengthMin());

        ps.updateValueLength("foobar");
        assertEquals(6, ps.getValueLengthTotal());
        assertEquals(6, ps.getValueLengthMax());
        assertEquals(6, ps.getValueLengthMin());

        ps.updateValueLength("foo");
        assertEquals(9, ps.getValueLengthTotal());
        assertEquals(6, ps.getValueLengthMax());
        assertEquals(3, ps.getValueLengthMin());
    }

    @Test
    public void testUpdateHll() {
        String value;
        for (int i = 0; i < 100; i++) {
            value = "foobar" + i;
            long hash = Hash.hash64(value.hashCode());
            ps.updateHll(hash);
        }
        assertTrue(ps.getHll().estimate() < 100);
    }
}
