package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil.getIndexRoot;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditorProvider.DEFAULT_PROPERTY_CMS_ROWS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        NodeState indexNode = getIndexRoot(nodeStore);
        NodeState statIndexNode = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexNode);

        utility.addNodes();

        assertTrue(statIndexNode.exists());

    }

    @Test
    public void testGetTopKNodeNotExists() {
        List<TopKValues.ValueCountPair> empty = StatisticsIndexHelper.getTopValues(
                "jcr:isAbstract", getIndexRoot(nodeStore));

        assertTrue(empty.isEmpty());
    }

    @Test
    public void testEmptyTopK() {
        TopKValues topKValues = StatisticsEditor.readTopKElements(null, null,
                                                                  5);
        assertTrue(topKValues.isEmpty());
    }

    @Test
    public void testPropertySketchIsStored() throws CommitFailedException {
        NodeState indexNode = getIndexRoot(nodeStore);
        NodeState statIndexNode = StatisticsIndexHelper.getNodeFromIndexRoot(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statIndexNode.exists());

        utility.addNodes();
        indexNode = getIndexRoot(nodeStore);
        statIndexNode = StatisticsIndexHelper.getNodeFromIndexRoot(indexNode);

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
        for (int i = 0; i < DEFAULT_PROPERTY_CMS_ROWS; i++) {
            assertTrue(t.hasProperty(StatisticsEditor.PROPERTY_CMS_NAME + i));
        }

        t.remove();
        utility.commit();
        assertFalse(nodeExists(t.getPath()));
    }

    @Test
    public void testProviderFailWithDifferentType() throws CommitFailedException {
        NodeState ns = nodeStore.getRoot();
        NodeBuilder b = ns.builder();

        IndexEditorProvider statProvider = new StatisticsEditorProvider();
        Editor editor = statProvider.getIndexEditor("fail", b, ns,
                                                    IndexUpdateCallback.NOOP);
        assertNull(editor);
    }

    @Test
    public void testProviderUsesDefaultValues() throws CommitFailedException {
        NodeState root = nodeStore.getRoot();
        NodeBuilder b = EMPTY_NODE.builder();
        b.setProperty(StatisticsEditorProvider.COMMON_PROPERTY_THRESHOLD, 10);
        b.setProperty(StatisticsEditorProvider.VALUE_CMS_COLS,
                      StatisticsEditorProvider.DEFAULT_VALUE_CMS_COLS);
        b.setProperty(StatisticsEditorProvider.VALUE_CMS_ROWS,
                      StatisticsEditorProvider.DEFAULT_VALUE_CMS_ROWS);

        b.setProperty(StatisticsEditorProvider.PROPERTY_CMS_COLS,
                      StatisticsEditorProvider.DEFAULT_PROPERTY_CMS_COLS);
        b.setProperty(StatisticsEditorProvider.PROPERTY_CMS_ROWS,
                      StatisticsEditorProvider.DEFAULT_PROPERTY_CMS_ROWS);

        IndexEditorProvider statProvider = new StatisticsEditorProvider();
        Editor editor = statProvider.getIndexEditor(
                StatisticsEditorProvider.TYPE, b, root,
                IndexUpdateCallback.NOOP);

        assertNotNull(editor);
        editor = editor.childNodeDeleted("foo1", root);
        assertNotNull(editor);
        String p = "bar";
        PropertyState ps = root.getProperty(p);
        editor.propertyDeleted(ps);
    }


    private boolean nodeExists(String path) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }
}
