package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

public class StatisticsIndexTest {

	Whiteboard wb;
	NodeStore nodeStore;
	Root root;
	QueryEngine qe;
	ContentSession session;

	@Before
	public void before() throws Exception {
		session = createRepository().login(null, null);
		root = session.getLatestRoot();
		qe = root.getQueryEngine();
	}

	@Test
	public void testStatisticsAreStored() throws Exception {
		String path = "/oak:index/statistics";
//		Tree r = root.getTree(path);
//		String pathToTest = "/index/properties/"
		String pathToCount = "/index/properties/propertiesCountMinSketch";
		String pathToHll = "/index/properties/uniqueHLL";

//		root.commit();
//		runAsyncIndex();

		addNodesWithProperties(path, 10);

//		root.getTree(pathToHll)
		Tree cmsTree = root.getTree(path + pathToCount);
		Tree hllTree = root.getTree(path + pathToHll);

		assertTrue(nodeExists("oak:index/statistics/index"));
		assertTrue(nodeExists(cmsTree.getPath()));
		assertTrue(nodeExists(hllTree.getPath()));

	}

	private void addNodesWithProperties(String path, int maxIter) throws CommitFailedException {
		Random r = new Random();
		int maxNum = 10;
		int num = r.nextInt(maxNum);
//		data[i] = r.nextInt(1 << num);
		for (int i = 0; i < 10; i++) {
			assertTrue("index not ready after " + maxIter + " iterations", i < maxIter);
			Tree t = root.getTree(path).addChild("test-" + i);
			for (int j = 0; j < 5; j++) {
				t.addChild("prop-" + j).setProperty("ABC", r.nextInt(10));
			}
			root.commit();
			runAsyncIndex();
		}
	}

	@Test
	public void testNotUsedBeforeValid() throws Exception {
		String path = "/oak:index/statistics";
		root.getTree(path).setProperty("resolution", 100);
		root.commit();

		// no index data before indexing
		assertFalse(nodeExists("oak:index/statistics/index"));
		assertTrue(nodeExists("oak:index/statistics"));
		// so, cost for traversal is high
		assertTrue(getCost("/jcr:root//*") >= 1.0E8);

		runAsyncIndex();
		// sometimes, the :index node doesn't exist because there are very few
		// nodes (randomly, because the seed value of the node statistics is random
		// by design) - so we create nodes until the index exists
		// (we could use a fixed seed to ensure this is not the case,
		// but creating nodes has the same effect)
		String s = root.getTree(path + "/index/properties/jcr:valueConstraints").toString();
//        System.out.println(s);
//        System.out.println("hello");

		System.out.println(nodeExists("oak:index/statistics/index"));
		int maxIter = 100;
		for (int i = 0; !nodeExists("oak:index/statistics/index"); i++) {
			assertTrue("index not ready after " + maxIter + " iterations", i < maxIter);
			Tree t = root.getTree(path).addChild("test" + i);
			for (int j = 0; j < 100; j++) {
				t.addChild("n" + j).setProperty("ABC", 1);
				;
//                System.out.println("HELLO");
			}
			root.commit();
			runAsyncIndex();
		}

		for (int i = 0; i < 5; i++) {
			assertTrue("index not ready after " + maxIter + " iterations", i < maxIter);
			Tree t = root.getTree(path).addChild("test-" + i);
			for (int j = 0; j < 100; j++) {
				t.addChild("ns" + j).setProperty("ABC", j);
			}
			root.commit();
			runAsyncIndex();
		}

		for (int i = 0; i < 5; i++) {
			assertTrue("index not ready after " + maxIter + " iterations", i < maxIter);
			Tree t = root.getTree(path).addChild("test2-" + i);
			for (int j = 0; j < 420; j++) {
				t.addChild("ns" + j).setProperty("KFC", j);
			}
			root.commit();
			runAsyncIndex();
		}
		// TODO: check that the tree follows the desired structure.
//        String s = root.getTree(path).toString();

		// remove the statistics index
		root.getTree(path).remove();
		root.commit();

		assertFalse(nodeExists(path));
	}

	private double getCost(String xpath) throws ParseException {
		String plan = executeXPathQuery("explain measure " + xpath);
		String cost = plan.substring(plan.lastIndexOf('{'));
		JsonObject json = parseJson(cost);
		double c = Double.parseDouble(json.getProperties().get("a"));
		return c;
	}

	private static JsonObject parseJson(String json) {
		JsopTokenizer t = new JsopTokenizer(json);
		t.read('{');
		return JsonObject.create(t);
	}

	private boolean nodeExists(String path) {
		return NodeStateUtils.getNode(nodeStore.getRoot(), path).exists();
	}

	protected String executeXPathQuery(String statement) throws ParseException {
		Result result = qe.executeQuery(statement, "xpath", null, NO_MAPPINGS);
		StringBuilder buff = new StringBuilder();
		for (ResultRow row : result.getRows()) {
			for (PropertyValue v : row.getValues()) {
				buff.append(v);
			}
		}
		return buff.toString();
	}

	protected ContentRepository createRepository() throws CommitFailedException {
		nodeStore = new MemoryNodeStore();
		Oak oak = new Oak(nodeStore).with(new InitialContent()).with(new OpenSecurityProvider())
				.with(new PropertyIndexEditorProvider()).with(new StatisticsEditorProvider())
//                .with(new NodeCounterEditorProvider())
				// Effectively disable async indexing auto run
				// such that we can control run timing as per test requirement
				.withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

//        mergeIndex("statistics");
		NodeBuilder builder = nodeStore.getRoot().builder();
		NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

		index.child("statistics").setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
				.setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
				.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, IndexConstants.ASYNC_PROPERTY_NAME)
				.setProperty("info", "STATISTICS");

		nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
		wb = oak.getWhiteboard();

		return oak.createContentRepository();
	}

	private void mergeIndex(String name) throws CommitFailedException {
		NodeBuilder builder = nodeStore.getRoot().builder();
		NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

		index.child(name).setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
				.setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
				.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, IndexConstants.ASYNC_PROPERTY_NAME)
				.setProperty("info", "STATISTICS");

		nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
	}

	private void runAsyncIndex() {
		Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
			@Override
			public boolean test(@Nullable Runnable input) {
				return input instanceof AsyncIndexUpdate;
			}
		});
		assertNotNull(async);
		async.run();
		root.refresh();
	}
}
