package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeReaderTest {
    NodeStore store;
    TestUtility utility;

    @Before
    public void before() throws CommitFailedException,
            NoSuchWorkspaceException, LoginException {
        store = new MemoryNodeStore();
        utility = new TestUtility(store, "statistics");
    }

    @Test
    public void testGetStatisticsIndexNode() throws CommitFailedException {
        NodeState indexNode = NodeReader.getIndexRoot(store);
        Optional<NodeState> statNode =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.isPresent());

        utility.addNodes();
        indexNode = NodeReader.getIndexRoot(store);
        statNode = NodeReader.getStatisticsIndexDataNodeOrNull(indexNode);

        // since we added some properties and re-read the index it should now
        // exist. We don't verify that the properties actually exist as that
        // logic belongs to the StatisticsEditor
        assertTrue(statNode.isPresent() && statNode.get().exists());
    }

    @Test
    public void testGetStringOrEmptyWithValidString() throws CommitFailedException {
        utility.addNodes();
        NodeState indexNode = NodeReader.getIndexRoot(store);
        Optional<NodeState> statNode =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexNode);

        assertTrue(statNode.isPresent());
        NodeState statNodeIndex = statNode.get();
        // since some nodes have been added we know that a property sketch
        // has been created with at least one row
        String val = NodeReader.getStringOrEmpty(statNodeIndex,
                                                 StatisticsEditor.PROPERTY_CMS_NAME + 0);
        assertFalse(val.isEmpty());
    }

    @Test
    public void testGetStatNodeWithNonExistentIndexRoot() {
        NodeState indexRoot = NodeReader.getIndexRoot(store);
        NodeState empty = indexRoot.getChildNode("DOES NOT EXIST");
        Optional<NodeState> stat = NodeReader.getStatisticsIndexDataNodeOrNull(
                empty);

        assertTrue(stat.isEmpty());
    }

    @Test
    public void testShouldBeEmptyBecauseEmptyStatIndex() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");

        // remove the oak:index/statistics node
        indexRootBuilder.getChildNode(StatisticsEditorProvider.TYPE).remove();

        NodeState indexRoot = indexRootBuilder.getNodeState();

        Optional<NodeState> result =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexRoot);

        assertTrue(result.isEmpty());
    }

    @Test
    public void testShouldBeEmptyBecauseInvalidStatType() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");

        // set the type property to a value that is not "statistics"
        indexRootBuilder.getChildNode(StatisticsEditorProvider.TYPE)
                        .setProperty("type", "INVALID");
        NodeState indexRoot = indexRootBuilder.getNodeState();

        Optional<NodeState> result =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexRoot);

        assertTrue(result.isEmpty());
    }


}
