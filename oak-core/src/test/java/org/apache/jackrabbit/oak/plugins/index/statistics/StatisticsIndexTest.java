package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class StatisticsIndexTest {

    NodeStore nodeStore;
    Root root;

    private TestUtility utility;

    private static JsonObject parseJson(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        return JsonObject.create(t);
    }

    @Before
    public void before() throws Exception {
        nodeStore = new MemoryNodeStore();
        utility = new TestUtility(nodeStore, "statistics");
    }

    @Test
    public void testCorruptedIndex() throws CommitFailedException {
        utility.addNodes();
        Tree t = utility.getIndexNodeTree();
    }

    @Test
    public void testGetValueAsString() throws CommitFailedException {
        String veryLong = "this is a long string".repeat(10);
        String veryLongTruncated = StatisticsEditor.getValueAsString(veryLong);
        assertNotEquals(veryLong, veryLongTruncated);

        Object expected = "shortString";
        String actual = StatisticsEditor.getValueAsString(expected);
        assertEquals(expected, actual);
    }

    @Test
    public void testReadProperty() throws CommitFailedException {
        utility.addNodes();

        NodeBuilder builder = nodeStore.getRoot().builder();

        builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
               .getChildNode(StatisticsEditorProvider.TYPE)
               .getChildNode("properties")
               .remove();

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Optional<PropertyStatistics> prop =
                StatisticsEditor.readPropertyStatistics(
                builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
                       .getChildNode(StatisticsEditorProvider.TYPE),
                "jcr:primaryType");

        assertTrue(prop.isEmpty());
    }

    @Test
    public void testEmptyTopK() {
        TopKValues topKValues = StatisticsEditor.readTopKElements(null, null,
                                                                  5);
        assertTrue(topKValues.isEmpty());
    }

    @Test
    public void testPropertySketchIsStored() throws CommitFailedException {
        NodeState indexNode = NodeReader.getIndexRoot(nodeStore);
        Optional<NodeState> statNode =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.isPresent());

        utility.addNodes();
        indexNode = NodeReader.getIndexRoot(nodeStore);
        statNode = NodeReader.getStatisticsIndexDataNodeOrNull(indexNode);

        // since we added *some* properties (note that it doesn't really matter
        // which ones) and re-read the index it should now exist.
        assertTrue(statNode.isPresent() && statNode.get().exists());

        NodeState statIndexNode = statNode.get();

        System.out.println(statIndexNode);
        assertTrue(statIndexNode.hasProperty(
                StatisticsEditor.PROPERTY_CMS_ROWS_NAME));
        assertTrue(statIndexNode.hasProperty(
                StatisticsEditor.PROPERTY_CMS_COLS_NAME));

        int rows = Math.toIntExact(getLong(statIndexNode,
                                           StatisticsEditor.PROPERTY_CMS_ROWS_NAME));
        for (int i = 0; i < rows; i++) {
            assertTrue(statIndexNode.hasProperty(
                    StatisticsEditor.PROPERTY_CMS_NAME + i));
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
