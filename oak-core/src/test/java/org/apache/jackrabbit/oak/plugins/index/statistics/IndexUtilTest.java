package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.Set;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getIndexRoot;
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getPropertiesOf;
import static org.apache.jackrabbit.oak.plugins.index.statistics.TestUtility.createTestIndex;
import static org.apache.jackrabbit.oak.plugins.index.statistics.TestUtility.setProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexUtilTest {
    TestUtility utility;
    NodeStore store;
    private NodeState root;
    private NodeBuilder rootBuilder;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException, CommitFailedException {
        store = new MemoryNodeStore();
        root = INITIAL_CONTENT;
        rootBuilder = root.builder();
        utility = new TestUtility(store, "statistics");
    }

    @Test
    public void getIndexedNode() throws CommitFailedException {
        // create the statistics index before we try to get it
        utility.addNodes();

        NodeState node = IndexUtil.getIndexedNodeFromName("statistics", store);
        assertTrue(node.exists());
    }


    @Test
    public void testParse() {
        String s = "some/arbitrary/path";
        String res = IndexUtil.parse(s);
        assertEquals("path", res);

        String empty = IndexUtil.parse("");
        assertEquals("", empty);

        String longPath = "long_property_name@#$";
        String onePath = IndexUtil.parse(longPath);
        assertEquals(longPath, onePath);
    }

    @Test
    public void testGetNamesValidNode() throws CommitFailedException {
        NodeBuilder testIndex = createTestIndex("testIndex", rootBuilder);
        setProperty(testIndex, "properties/foo", "name", "foo");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // now we have created a test index that is similar in structure as
        // socialLucene etc.
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getPropertiesOf(indexNode);
        assertFalse(names.isEmpty());
    }

    @Test
    public void testGetNamesMissingNode() throws CommitFailedException {
        NodeBuilder testIndex = createTestIndex("testIndex", rootBuilder);
        testIndex.child("nt:hierarchyNode");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // at this point, the expected children of the node doesn't exists.
        // So it should return an empty list
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getPropertiesOf(indexNode);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testShouldBeEmptyWithVirtualProperty() throws CommitFailedException {
        // adding virtual property to node
        TestUtility.addNodeWithProperty(":nodeName", rootBuilder, store);

        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getPropertiesOf(indexNode);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetProperty() throws CommitFailedException {
        TestUtility.addNodeWithProperty("something", rootBuilder, store);
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getPropertiesOf(indexNode);
        assertFalse(names.isEmpty());
    }

    @Test
    public void testGetNamesWithInvalidNodeState() {
        Set<String> names = getPropertiesOf(null);
        assertTrue(names.isEmpty());

        names = getPropertiesOf(EmptyNodeState.MISSING_NODE);
        assertTrue(names.isEmpty());

    }
}
