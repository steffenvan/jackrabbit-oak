package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StatisticsIndexTest {

    NodeStore nodeStore;

    private TestUtility utility;

    @Before
    public void before() throws Exception {
        nodeStore = new MemoryNodeStore();
        utility = new TestUtility(nodeStore, "statistics");
    }

    @Test
    public void testGetValueAsString() {
        String veryLong = "this is a long string".repeat(10);
        String veryLongTruncated = StatisticsEditor.getValueAsString(veryLong);
        assertNotEquals(veryLong, veryLongTruncated);

        Object expected = "shortString";
        String actual = StatisticsEditor.getValueAsString(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testMultipleAdds() throws CommitFailedException {

        utility.addNodes();
        NodeState indexNode = IndexReader.getIndexRoot(nodeStore);
        NodeState statIndexNode =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        utility.addNodes();

        assertTrue(statIndexNode.exists());

    }

    @Test
    public void testEmptyTopK() {
        TopKValues topKValues = StatisticsEditor.readTopKElements(null, null,
                                                                  5);
        assertTrue(topKValues.isEmpty());
    }

    @Test
    public void testPropertySketchIsStored() throws CommitFailedException {
        NodeState indexNode = IndexReader.getIndexRoot(nodeStore);
        NodeState statIndexNode =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statIndexNode.exists());

        utility.addNodes();
        indexNode = IndexReader.getIndexRoot(nodeStore);
        statIndexNode =
                IndexReader.getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                indexNode);

        // since we added *some* properties (note that it doesn't really matter
        // which ones) and re-read the index it should now exist.
        assertTrue(statIndexNode.exists());

        statIndexNode = statIndexNode.getChildNode("jcr:isAbstract");
        int rows = Math.toIntExact(
                getLong(statIndexNode, StatisticsEditor.VALUE_SKETCH_ROWS));
        for (int i = 0; i < rows; i++) {
            assertTrue(statIndexNode.hasProperty(
                    StatisticsEditor.VALUE_SKETCH_NAME + i));
        }

    }

    @Test
    public void testPropertyCMSIsStored() throws Exception {
        utility.addNodes();


        Tree t = utility.getIndexNodeTree();
        // the index should be created after we add the properties.
        assertTrue(nodeExists(t.getPath()));

        assertTrue(t.hasProperty(StatisticsEditor.PROPERTY_CMS_ROWS_NAME));
        assertTrue(t.hasProperty(StatisticsEditor.PROPERTY_CMS_COLS_NAME));

        // since we aren't doing any modifications to the construction of the
        // Count-Min sketch, we know that the one that the one that is
        // created after we add the properties will have the default number
        // of rows
        int rows = StatisticsEditorProvider.DEFAULT_PROPERTY_CMS_ROWS;
        for (int i = 0; i < rows; i++) {
            assertTrue(t.hasProperty(StatisticsEditor.PROPERTY_CMS_NAME + i));
        }

        t.remove();
        utility.commit();
        assertFalse(nodeExists(t.getPath()));
    }

    private boolean nodeExists(String path) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }
}
