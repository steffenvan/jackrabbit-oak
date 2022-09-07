package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.StatisticsDefinitionPrinter;
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
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

public class StatisticsDefinitionPrinterTest {

	Whiteboard wb;
	NodeStore store;
	Root root;
	QueryEngine qe;
	ContentSession session;
	StatisticsDefinitionPrinter printer;

	@Before
	public void before() throws Exception {
		store = new MemoryNodeStore();
		session = createRepository(store).login(null, null);
		root = session.getLatestRoot();
		qe = root.getQueryEngine();
		printer = new StatisticsDefinitionPrinter(store);
	}

	@Test
	public void testStatPrinter() throws CommitFailedException, ParseException {

		runAsyncIndex();
		assertTrue(nodeExists("oak:index/statistics/index"));
		assertTrue(nodeExists("oak:index/statistics/index/properties"));
		assertTrue(nodeExists("oak:index/statistics/index/properties/jcr:primaryType"));

		String json = getStatsIndexInfo();
		// removing the first and last character as they contain an extra '{' and '}'
		// which the JSON simple parser complains about.
		json = json.substring(1, json.length() - 1);
		System.out.println(json);

		Map<String, Object> map = getJson(json);

		assertTrue(((Map<String, Object>) map.get("properties")).containsKey("jcr:valueConstraints"));

	}

	private Map<String, Object> getJson(String json) throws ParseException {
		JSONParser parser = new JSONParser();
		ContainerFactory orderedKeyFactory = new ContainerFactory() {
			@Override
			public List<Object> creatArrayContainer() {
				return new LinkedList<Object>();
			}

			@Override
			public Map<Object, Object> createObjectContainer() {
				return new LinkedHashMap<Object, Object>();
			}

		};

		Object obj = parser.parse(json, orderedKeyFactory);
		@SuppressWarnings("unchecked")
		LinkedHashMap<String, Object> map = (LinkedHashMap<String, Object>) obj;

		return map;

	}

	private boolean nodeExists(String path) {
		return NodeStateUtils.getNode(store.getRoot(), path).exists();
	}

	private String getStatsIndexInfo() throws CommitFailedException {
//		StatisticsDefinitionPrinter printer = new StatisticsDefinitionPrinter(store);
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		printer.print(pw, Format.JSON, false);

		pw.flush();

		return sw.toString();
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

	protected ContentRepository createRepository(NodeStore store) throws CommitFailedException {
		Oak oak = new Oak(store).with(new InitialContent()).with(new OpenSecurityProvider())
				.with(new PropertyIndexEditorProvider()).with(new StatisticsEditorProvider())
				// Effectively disable async indexing auto run
				// such that we can control run timing as per test requirement
				.withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

		mergeIndex("statistics");

		NodeBuilder builder = store.getRoot().builder();
		store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
		wb = oak.getWhiteboard();

		return oak.createContentRepository();
	}

	private void mergeIndex(String name) throws CommitFailedException {
		NodeBuilder builder = store.getRoot().builder();
		NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

		index.child(name).setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
				.setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
				.setProperty(IndexConstants.ASYNC_PROPERTY_NAME, IndexConstants.ASYNC_PROPERTY_NAME)
				.setProperty("info", "STATISTICS");

		store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
	}

}
