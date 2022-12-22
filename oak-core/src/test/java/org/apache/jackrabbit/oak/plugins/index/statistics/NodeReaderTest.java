package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NodeReaderTest {
    NodeStore store;
    Whiteboard wb;
    ContentSession session;
    Root root;

    @Before
    public void before() throws CommitFailedException,
            NoSuchWorkspaceException, LoginException {
        store = new MemoryNodeStore();
        session = createContentRepository().login(null, null);
        root = session.getLatestRoot();
    }

    @Test
    public void testGetStatisticsIndexNode() throws CommitFailedException {
        NodeState indexNode = NodeReader.getIndexNode(store);
        Optional<NodeState> statNode =
                NodeReader.getStatisticsIndexDataNodeOrNull(
                indexNode);

        // no properties added so it should be empty
        assertFalse(statNode.isPresent());

        addProperties();
        indexNode = NodeReader.getIndexNode(store);
        statNode = NodeReader.getStatisticsIndexDataNodeOrNull(indexNode);

        // after adding some properties the statistics index should exist
        assertTrue(statNode.isPresent());

        // not ideal, but because of the previous assertion we know this
        // the Optional isn't empty
        NodeState statIndex = statNode.get();

        // since we added some properties and re-read the index it should now
        // exist. We don't verify that the properties actually exist as that
        // logic belongs to the StatisticsEditor
        assertTrue(statIndex.exists());

    }

    private void addProperties() throws CommitFailedException {
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

    private ContentRepository createContentRepository() throws CommitFailedException {
        store = new MemoryNodeStore();
        Oak oak = new Oak(store).with(new InitialContent())
                                .with(new OpenSecurityProvider())
                                .with(new PropertyIndexEditorProvider())
                                .with(new StatisticsEditorProvider())
                                // Effectively disable async indexing
                                // auto run such that we can control run
                                // timing as per test requirement
                                .withAsyncIndexing("async",
                                                   TimeUnit.DAYS.toSeconds(1));

        //        mergeIndex("statistics");
        NodeBuilder builder = store.getRoot().builder();
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

        index.child("statistics")
             .setProperty(JcrConstants.JCR_PRIMARYTYPE,
                          INDEX_DEFINITIONS_NODE_TYPE, NAME)
             .setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
             .setProperty(IndexConstants.ASYNC_PROPERTY_NAME,
                          IndexConstants.ASYNC_PROPERTY_NAME)
             .setProperty("info", "STATISTICS");

        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        wb = oak.getWhiteboard();

        return oak.createContentRepository();
    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class,
                                                    (Predicate<Runnable>) input -> input instanceof AsyncIndexUpdate);
        assertNotNull(async);
        async.run();
        root.refresh();
    }

}
