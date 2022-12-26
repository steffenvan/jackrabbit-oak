package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
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
import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getNames;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexUtilTest {

    private final String INDEX_RULES = "indexRules";
    private final String PROP_NAME = "name";
    TestUtility utility;
    NodeStore store;
    private NodeState root;
    private NodeBuilder rootBuilder;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException,
            CommitFailedException {
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
        String s = "very/long/path/";
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
        NodeBuilder testIndex = rootBuilder.child(
                                                   IndexConstants.INDEX_DEFINITIONS_NAME)
                                           .child("testIndex")
                                           .child(INDEX_RULES);


        rootBuilder.setProperty(PROP_NAME, "testIndex");
        NodeBuilder name = testIndex.child("nt:hierarchyNode");
        name.setProperty("name", "foo");
        name.child("properties").child("foo").setProperty("name", "foo");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // now we have created a test index that is similar in structure as
        // socialLucene etc.
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertFalse(names.isEmpty());
    }

    @Test
    public void testGetNamesMissingNode() throws CommitFailedException {
        NodeBuilder testIndex = rootBuilder.child(
                                                   IndexConstants.INDEX_DEFINITIONS_NAME)
                                           .child("testIndex")
                                           .child(INDEX_RULES);


        rootBuilder.setProperty(PROP_NAME, "testIndex");
        testIndex.child("nt:hierarchyNode");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // at this point, the expected children of the node doesn't exists.
        // So it should return an empty list
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testShouldBeEmptyWithVirtualProperty() throws CommitFailedException {
        // adding virtual property to node
        addNodeWithProperty(":nodeName");

        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testGetProperty() throws CommitFailedException {
        addNodeWithProperty("something");
        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertFalse(names.isEmpty());
    }

    @Test
    public void testGetNamesWithInvalidNodeState() {
        Set<String> names = getNames(null);
        assertTrue(names.isEmpty());

        names = getNames(EmptyNodeState.MISSING_NODE);
        assertTrue(names.isEmpty());

    }

    private void addNodeWithProperty(String name) throws CommitFailedException {
        NodeBuilder testIndex = rootBuilder.child(
                                                   IndexConstants.INDEX_DEFINITIONS_NAME)
                                           .child("testIndex")
                                           .child(INDEX_RULES);


        rootBuilder.setProperty(PROP_NAME, "testIndex");
        NodeBuilder node = testIndex.child("nt:hierarchyNode");
        NodeBuilder foo = node.child("properties").child("foo");

        // a is a virtual property if the properties "property" is ":nodeName"
        foo.setProperty("properties", name);
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
