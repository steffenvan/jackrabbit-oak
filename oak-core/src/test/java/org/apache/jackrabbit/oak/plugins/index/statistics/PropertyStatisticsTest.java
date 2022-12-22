package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertyStatisticsTest {
    PropertyStatistics ps;

    @Before
    public void before() {
        String name = "foo";
        int m = 64;
        HyperLogLog hll = new HyperLogLog(64);
        CountMinSketch cms = new CountMinSketch(5, 16);
        TopKValues topValues = new TopKValues(5);
        ps = new PropertyStatistics(name, 5L, hll, cms, topValues);
    }

    @After
    public void after() {
        ps = null;
    }

    @Test
    public void testCreation() {

    }

    @Test
    public void testToString() {

    }

    @Test
    public void testInc() {

    }

    @Test
    public void testUpdateValues() {
        String value = "foo";
        for (int i = 0; i < 100; i++) {
            if (i % 10 == 0) {
                String val = "bar";
                ps.updateValueCount(val, Hash.hash64(val.hashCode()));
            }
            ps.updateValueCount(value, Hash.hash64(value.hashCode()));
        }
        long hash = Hash.hash64(value.hashCode());
        long cmsCount = ps.getCmsCount(hash);
        System.out.println(cmsCount);

        assertTrue(cmsCount >= 100);
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
