package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StatisticsIndexTest {

    Whiteboard wb;
    NodeStore nodeStore;
    Root root;
    QueryEngine qe;
    ContentSession session;

    private static JsonObject parseJson(String json) {
        JsopTokenizer t = new JsopTokenizer(json);
        t.read('{');
        return JsonObject.create(t);
    }

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
    }

    private void addNodes() throws CommitFailedException {
        // add some properties so that the statistics node gets updated
        for (int i = 0; i < 5; i++) {
            Tree t = root.getTree("/").addChild("foo" + i);
            for (int j = 0; j < 2; j++) {
                t.addChild("bar" + j);
            }

            root.commit();
            runAsyncIndex();
        }
    }

    private void addPropertyWithLongName() throws CommitFailedException {
        for (int i = 0; i < 5; i++) {
            Tree t = root.getTree("/").addChild("foo" + i);
            for (int j = 0; j < 2; j++) {
                t.addChild("bar".repeat(50));
            }

            root.commit();
            runAsyncIndex();
        }
    }

    @Test
    public void testCorruptedIndex() throws CommitFailedException {
        addNodes();
        Tree t = getIndexNodeTree();

        //        builder.setProperty();
    }

    @Test
    public void testLongValueName() throws CommitFailedException {
        addPropertyWithLongName();
        Tree t = getIndexNodeTree();

        //        assertTrue(t)
    }

    @Test
    public void testPropertySketchIsStored() throws CommitFailedException {
        NodeState indexNode = NodeReader.getIndexNode(nodeStore);
        Optional<NodeState> statNode =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.isPresent());

        addNodes();
        indexNode = NodeReader.getIndexNode(nodeStore);
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

    private Tree getIndexNodeTree() {
        String path = "/oak:index/statistics/index";
        return root.getTree(path);
    }

    @Test
    public void testPropertyCMSIsStored() throws Exception {
        addNodes();


        Tree t = getIndexNodeTree();
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
        root.commit();
        assertFalse(nodeExists(t.getPath()));
    }

    private boolean nodeExists(String path) {
        return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
    }

    protected ContentRepository createRepository() throws CommitFailedException {
        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore).with(new InitialContent())
                                    .with(new OpenSecurityProvider())
                                    .with(new PropertyIndexEditorProvider())
                                    .with(new StatisticsEditorProvider())
                                    .with(new NodeCounterEditorProvider())
                                    // Effectively disable async indexing
                                    // auto run such that we can control run
                                    // timing as per test requirement
                                    .withAsyncIndexing("async",
                                                       TimeUnit.DAYS.toSeconds(
                                                               1));

        mergeIndex("statistics");
        wb = oak.getWhiteboard();

        return oak.createContentRepository();
    }

    private void mergeIndex(String name) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

        index.child(name)
             .setProperty(JcrConstants.JCR_PRIMARYTYPE,
                          INDEX_DEFINITIONS_NODE_TYPE, NAME)
             .setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
             .setProperty(IndexConstants.ASYNC_PROPERTY_NAME,
                          IndexConstants.ASYNC_PROPERTY_NAME)
             .setProperty("info", "STATISTICS");

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class,
                                                    (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate);
        assertNotNull(async);
        async.run();
        root.refresh();
    }
}
