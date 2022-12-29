package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
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
import java.util.Optional;
import java.util.Set;

import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.plugins.index.statistics.TestUtility.addNodeWithProperty;
import static org.apache.jackrabbit.oak.plugins.index.statistics.TestUtility.createTestIndex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContentStatisticsTest {
    private NodeStore store;
    private ContentStatistics mbean;

    private NodeState root;
    private NodeBuilder rootBuilder;
    private TestUtility testUtil;

    @Before
    public void before() throws NoSuchWorkspaceException, LoginException,
            CommitFailedException {
        store = new MemoryNodeStore();
        testUtil = new TestUtility(store, "statistics");
        mbean = new ContentStatistics(store);
        root = INITIAL_CONTENT;
        rootBuilder = root.builder();
    }

    @Test
    public void testGetSinglePropertyEstimation() throws CommitFailedException {
        testUtil.addNodes();
        String property = "jcr:primaryType";
        Optional<PropertyStatistics> actual = mbean.getSinglePropertyEstimation(
                property);

        assertTrue(actual.isPresent());
        assertTrue(actual.get().getUniqueCount() > 0);
    }

    @Test
    public void testGetAllPropertiesEstimation() throws CommitFailedException {
        testUtil.addNodes();
        String property = "jcr:primaryType";
        List<PropertyStatistics> actual = mbean.getAllPropertyStatistics();
        System.out.println(actual);
        assertFalse(actual.isEmpty());
    }

    @Test
    public void testEmptyIndex() {
        List<PropertyStatistics> actual = mbean.getAllPropertyStatistics();
        // since the statistics index hasn't been created, it should be empty
        assertTrue(actual.isEmpty());
    }

    @Test
    public void testGetPropertyNames() throws CommitFailedException {
        Set<String> propNames = mbean.getIndexedPropertyNames();
        // no index, should be empty
        assertTrue(propNames.isEmpty());

        // create a valid test index that follows the structure of other
        // indexes. Like e.g. SocialLucene.
        NodeBuilder testIndex = createTestIndex("testIndex", rootBuilder);
        NodeBuilder name = testIndex.child("nt:hierarchyNode");
        name.setProperty("name", "foo");
        name.child("properties").child("foo").setProperty("name", "foo");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        testUtil.addNodes();

        propNames = mbean.getIndexedPropertyNames();
        assertFalse(propNames.isEmpty());
    }

    @Test
    public void testTopKValues() {
        String prop = "jcr:primaryType";

        List<TopKValues.ProportionInfo> actual =
                mbean.getValueProportionInfoForSingleProperty(
                prop);
    }

    @Test
    public void testGetProportionalInfoForSingleProperty() throws CommitFailedException {
        String prop = "jcr:primaryType";
        testUtil.addNodes();

        List<TopKValues.ProportionInfo> actual =
                mbean.getValueProportionInfoForSingleProperty(
                prop);

        assertFalse(actual.isEmpty());

        List<List<TopKValues.ProportionInfo>> all =
                mbean.getProportionInfoForIndexedProperties();
        assertFalse(all.isEmpty());
    }

    @Test
    public void testGetPropertyNamesForSingleIndex() throws CommitFailedException {
        testUtil.addNodes();
        NodeBuilder testIndex = TestUtility.createTestIndex("testIndex",
                                                            rootBuilder);
        addNodeWithProperty("foo", rootBuilder, store);
        Set<String> names = mbean.getPropertyNamesForSingleIndex("testIndex");
        assertFalse(names.isEmpty());
        assertEquals(1, names.size());
        assertTrue(names.contains("foo"));
    }

    @Test
    public void getTopKValuesForSingleProperty() throws CommitFailedException {
        testUtil.addNodes();
        List<TopKValues.ValueCountPair> topValues =
                mbean.getTopKValuesForProperty(
                "jcr:primaryType", 5);

        assertFalse(topValues.isEmpty());

    }
}
