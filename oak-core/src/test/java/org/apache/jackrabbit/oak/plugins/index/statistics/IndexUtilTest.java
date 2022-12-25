package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexUtilTest {

    private final String INDEX_RULES = "indexRules";
    private final String PROP_NAME = "name";
    private final NodeState root = INITIAL_CONTENT;
    private final NodeBuilder rootBuilder = root.builder();
    TestUtility utility;
    NodeStore store;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException,
            CommitFailedException {
        store = new MemoryNodeStore();
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
    public void testGetNamesWithInvalidIndex() {
        NodeState indexNode = getIndexRoot(store).getChildNode("statistics");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertFalse(names.isEmpty());
    }

    @Test
    public void testHasProperties() throws CommitFailedException {
        NodeBuilder testIndex = rootBuilder.child(
                                                   IndexConstants.INDEX_DEFINITIONS_NAME)
                                           .child("testIndex")
                                           .child(INDEX_RULES);


        rootBuilder.setProperty(PROP_NAME, "testIndex");
        NodeBuilder name = testIndex.child("nt:hierarchyNode");
        //        name.setProperty("properties", ":nodeName");
        NodeBuilder foo = name.child("properties").child("foo");
        //        foo.removeProperty("name");
        foo.setProperty("properties", ":nodeName");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState indexNode = getIndexRoot(store).getChildNode("testIndex");
        Set<String> names = IndexUtil.getNames(indexNode);
        assertFalse(names.isEmpty());
    }
}
