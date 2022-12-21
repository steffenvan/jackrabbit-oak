package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.junit.Before;
import org.junit.Test;

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

    }

    @Test
    public void testUpdateValueLengths() {

    }

    @Test
    public void testUpdateHll() {

    }
}
