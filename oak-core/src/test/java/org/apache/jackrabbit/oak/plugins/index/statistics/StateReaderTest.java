package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class StateReaderTest {
    NodeStore store;

    @Before
    public void before() {
        store = new MemoryNodeStore();
    }

    @Test
    public void testGetLongOrZero() {
        PropertyState ps = null;
        StateReader.getLongOrZero(ps);
    }

    @Test
    public void testGetStringOrEmpty() {
        PropertyState ps = null;
        StateReader.getStringOrEmpty(ps);
    }

    @Test
    public void testGetIndexNode() {
        //        StateReader.getIndexNode()
    }
}
