package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
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
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getIndexRoot;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StatisticsIndexHelperTest {
    NodeStore store;
    TestUtility utility;

    @Before
    public void before() throws CommitFailedException,
            NoSuchWorkspaceException, LoginException {
        store = new MemoryNodeStore();
        utility = new TestUtility(store, "statistics");
    }

    @Test
    public void testGetCountStatNodeNotExists() {

        // should return -1 as the statistics node does not exist.
        long count = StatisticsIndexHelper.getCount("jcr:isAbstract",
                                                    getIndexRoot(store));

        assertEquals(-1, count);
    }

    @Test
    public void testGetCount() throws CommitFailedException {
        utility.addNodes();


        long count = StatisticsIndexHelper.getCount("jcr:isAbstract",
                                                    getIndexRoot(store));
        assertTrue(count != -1);
        assertTrue(count > 0);
    }

    @Test
    public void testGetTopK() throws CommitFailedException {
        utility.addNodes();
        List<TopKValues.ValueCountPair> topValues =
                StatisticsIndexHelper.getTopValues(
                "jcr:isAbstract", getIndexRoot(store));

        assertFalse(topValues.isEmpty());
    }

    @Test
    public void testFromProperty() throws CommitFailedException {
        utility.addNodes();

        NodeState s = StatisticsIndexHelper.getPropertyNodeAtStatisticsIndex(
                "jcr:isAbstract", getIndexRoot(store));
        assertTrue(s.exists());
    }

    @Test
    public void testGetStatisticsIndexNode() throws CommitFailedException {
        NodeState indexNode = getIndexRoot(store);
        NodeState statNode = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.exists());

        utility.addNodes();
        indexNode = getIndexRoot(store);
        statNode = StatisticsIndexHelper.getNodeFromIndexRoot(indexNode);

        // since we added some properties and re-read the index it should now
        // exist. We don't verify that the properties actually exist as that
        // logic belongs to the StatisticsEditor
        assertTrue(statNode.exists());
    }

    @Test
    public void testGetStringOrEmptyWithValidString() throws CommitFailedException {
        utility.addNodes();
        NodeState indexNode = getIndexRoot(store);
        NodeState statNodeIndex = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexNode);

        assertTrue(statNodeIndex.exists());

        // since some nodes have been added we know that a property sketch
        // has been created with at least one row
        String val = StatisticsIndexHelper.getStringOrEmpty(
                statNodeIndex.getChildNode("jcr:isAbstract"),
                StatisticsEditor.VALUE_SKETCH_NAME + 0);
        assertFalse(val.isEmpty());
    }

    @Test
    public void testGetStatNodeWithNonExistentIndexRoot() {
        NodeState indexRoot = getIndexRoot(store);
        NodeState empty = indexRoot.getChildNode("DOES NOT EXIST");
        NodeState stat = StatisticsIndexHelper.getNodeFromIndexRoot(empty);

        assertFalse(stat.exists());
    }

    @Test
    public void testShouldBeEmptyBecauseEmptyStatIndex() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");

        // remove the oak:index/statistics node
        indexRootBuilder.getChildNode(StatisticsEditorProvider.TYPE).remove();

        NodeState indexRoot = indexRootBuilder.getNodeState();

        NodeState result = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexRoot);

        assertFalse(result.exists());
    }

    @Test
    public void testShouldBeEmptyBecauseInvalidStatType() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");

        // set the type property to a value that is not "statistics"
        indexRootBuilder.getChildNode(StatisticsEditorProvider.TYPE)
                        .setProperty("type", "INVALID");
        NodeState indexRoot = indexRootBuilder.getNodeState();

        NodeState result = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexRoot);

        assertFalse(result.exists());
    }

    @Test
    public void testShouldReturnEmptyNode() throws CommitFailedException {
        utility.addNodes();
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");
        boolean removed = builder.getChildNode("oak:index")
                                 .getChildNode("statistics")
                                 .getChildNode("index")
                                 .getChildNode("properties")
                                 .remove();

        // remove the oak:index/statistics
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        NodeState indexRoot = indexRootBuilder.getNodeState();

        NodeState result = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexRoot);

        assertFalse(result.exists());
    }


}
