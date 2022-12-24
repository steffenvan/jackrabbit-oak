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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndexReaderTest {
    NodeStore store;
    TestUtility utility;

    @Before
    public void before() throws CommitFailedException,
            NoSuchWorkspaceException, LoginException {
        store = new MemoryNodeStore();
        utility = new TestUtility(store, "statistics");
    }

    @Test
    public void testFromProperty() throws CommitFailedException {
        utility.addNodes();

        NodeState s = IndexReader.getPropertyNode("jcr:isAbstract", store);
        assertTrue(s.exists());
    }

    @Test
    public void testGetStatisticsIndexNode() throws CommitFailedException {
        NodeState indexNode = IndexReader.getIndexRoot(store);
        NodeState statNode =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.exists());

        utility.addNodes();
        indexNode = IndexReader.getIndexRoot(store);
        statNode =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        // since we added some properties and re-read the index it should now
        // exist. We don't verify that the properties actually exist as that
        // logic belongs to the StatisticsEditor
        assertTrue(statNode.exists());
    }

    @Test
    public void testGetStringOrEmptyWithValidString() throws CommitFailedException {
        utility.addNodes();
        NodeState indexNode = IndexReader.getIndexRoot(store);
        NodeState statNodeIndex =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        assertTrue(statNodeIndex.exists());

        // since some nodes have been added we know that a property sketch
        // has been created with at least one row
        String val = IndexReader.getStringOrEmpty(
                statNodeIndex.getChildNode("jcr:isAbstract"),
                StatisticsEditor.VALUE_SKETCH_NAME + 0);
        assertFalse(val.isEmpty());
    }

    @Test
    public void testGetStatNodeWithNonExistentIndexRoot() {
        NodeState indexRoot = IndexReader.getIndexRoot(store);
        NodeState empty = indexRoot.getChildNode("DOES NOT EXIST");
        NodeState stat =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                empty);

        assertFalse(stat.exists());
    }

    @Test
    public void testShouldBeEmptyBecauseEmptyStatIndex() {
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder indexRootBuilder = builder.getChildNode("oak:index");

        // remove the oak:index/statistics node
        indexRootBuilder.getChildNode(StatisticsEditorProvider.TYPE).remove();

        NodeState indexRoot = indexRootBuilder.getNodeState();

        NodeState result =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
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

        NodeState result =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexRoot);

        assertFalse(result.exists());
    }


}
