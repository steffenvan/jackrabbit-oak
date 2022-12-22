package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
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
    public void testSetStatNodeWithInvalidIndex() {

    }
}
