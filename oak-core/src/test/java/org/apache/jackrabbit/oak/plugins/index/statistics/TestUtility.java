package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.statistics.state.export.CustomCSVReaderTest.INDEX_RULES;
import static org.junit.Assert.assertNotNull;

public class TestUtility {

    private static final String PROP_NAME = "properties";
    private final NodeStore store;
    private final Whiteboard wb;
    private final String indexName;
    private Root root;

    TestUtility(NodeStore store,
                String indexName) throws CommitFailedException,
            NoSuchWorkspaceException, LoginException {
        this.store = store;
        Oak oak = getOak();
        this.indexName = indexName;
        mergeIndex(store, indexName);
        this.wb = oak.getWhiteboard();
        ContentSession session = oak.createContentRepository()
                                    .login(null, null);
        this.root = session.getLatestRoot();
    }

    static NodeBuilder getPropertyFromBuilder(NodeStore store) {
        return store.getRoot()
                    .builder()
                    .getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
                    .getChildNode(StatisticsEditorProvider.TYPE)
                    .getChildNode(StatisticsEditor.DATA_NODE_NAME)
                    .getChildNode(StatisticsEditor.PROPERTIES);
    }

    public static NodeBuilder createTestIndex(String name,
                                              NodeBuilder builder) {
        return builder.child(IndexConstants.INDEX_DEFINITIONS_NAME)
                      .child(name)
                      .child(INDEX_RULES)
                      .setProperty(PROP_NAME, name);
    }

    static void setProperty(NodeBuilder builder, String path, String name,
                            String value) {
        for (String p : PathUtils.elements(path)) {
            builder = builder.child(p);
        }
        builder.setProperty(name, value);
    }

    /**
     * Adds a node at the path
     * "oak:index/testIndex/indexRules/properties/{@code name}" and sets the
     * "properties" property of that node to {@code name}.
     *
     * @param name    the property name
     * @param builder test class builder
     * @param store   test class store
     * @throws CommitFailedException if something fails in updating the node
     */
    static void addNodeWithProperty(String name, NodeBuilder builder,
                                    NodeStore store) throws CommitFailedException {
        NodeBuilder testIndex = createTestIndex("testIndex", builder);
        setProperty(testIndex, "nt:hierarchyNode/properties/foo", "properties",
                    name);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    Tree getIndexNodeTree() {
        String path = "/oak:index/" + indexName + "/index";
        return root.getTree(path);
    }

    public void addNodes() throws CommitFailedException {
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

    public void commit() throws CommitFailedException {
        root.commit();
    }

    public boolean nodeExists(String path) {
        return NodeStateUtils.getNode(store.getRoot(), path).exists();
    }

    private Oak getOak() {
        return new Oak(store).with(new InitialContent())
                             .with(new OpenSecurityProvider())
                             .with(new PropertyIndexEditorProvider())
                             .with(new StatisticsEditorProvider())
                             .with(new NodeCounterEditorProvider())
                             // Effectively disable async indexing
                             // auto run such that we can control run
                             // timing as per test requirement
                             .withAsyncIndexing("async",
                                                TimeUnit.DAYS.toSeconds(1));
    }

    Root getRoot() {
        return root;
    }

    private void mergeIndex(NodeStore nodeStore,
                            String indexName) throws CommitFailedException {
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

        index.child(indexName)
             .setProperty(JcrConstants.JCR_PRIMARYTYPE,
                          INDEX_DEFINITIONS_NODE_TYPE, NAME)
             .setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
             .setProperty(IndexConstants.ASYNC_PROPERTY_NAME,
                          IndexConstants.ASYNC_PROPERTY_NAME)
             .setProperty("info", "STATISTICS");

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class,
                                                    (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate);
        assertNotNull(async);
        async.run();
        root.refresh();
    }
}
