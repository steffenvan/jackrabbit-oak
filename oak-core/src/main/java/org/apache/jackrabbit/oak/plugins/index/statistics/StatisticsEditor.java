package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatisticsEditor implements Editor {

	private static final Logger LOG = LoggerFactory.getLogger(StatisticsEditor.class);

	public static final String DATA_NODE_NAME = "index";

	public static final int DEFAULT_RESOLUTION = 1000;
	public static final int DEFAULT_HLL_SIZE = 10;
	public static final int K_ELEMENTS = 5;

	public static final String PROPERTY_CMS_NAME = "propertiesCountMinSketch";
	public static final String PROPERTY_HLL_NAME = "uniqueHLL";
	public static final String PROPERTY_TOPK_NAME = "mostFrequentValueNames";
	public static final String PROPERTY_TOPK_COUNT = "mostFrequentValueCounts";
	public static final String PROPERTY_CMS_ROWS = "propertyCMSRows";
	public static final String PROPERTY_CMS_COLS = "propertyCMSCols";

	public static final String VALUE_SKETCH = "valueSketch";
	public static final String VALUE_SKETCH_ROWS = "valueSketchRows";
	public static final String VALUE_SKETCH_COLS = "valueSketchCols";

	private CountMinSketch propertyNameCMS;
	private Map<String, PropertyStatistics> propertyStatistics;
	private final StatisticsRoot root;
	private final StatisticsEditor parent;
	private final String name;
	private SipHash hash;
	private int recursionLevel;

	StatisticsEditor(StatisticsRoot root, CountMinSketch propertyNameCMS,
			Map<String, PropertyStatistics> propertyStatistics) {
		this.root = root;
		this.name = "/";
		this.parent = null;
		this.propertyNameCMS = propertyNameCMS;
		this.propertyStatistics = propertyStatistics;
		if (root.definition.hasChildNode(DATA_NODE_NAME)) {
			NodeBuilder data = root.definition.getChildNode(DATA_NODE_NAME);
			CountMinSketch cms = readCMS(data, "property", PROPERTY_CMS_ROWS, PROPERTY_CMS_COLS);
			if (cms != null) {
				this.propertyNameCMS = cms;
			}
		}
	}

	public CountMinSketch getCMS() {
		return this.propertyNameCMS;
	}

	private SipHash getHash() {
		if (hash != null) {
			return hash;
		}
		SipHash h;
		if (parent == null) {
			h = new SipHash(root.seed);
		} else {
			h = new SipHash(parent.getHash(), name.hashCode());
		}
		this.hash = h;
		return h;
	}

	@Override
	public void enter(NodeState before, NodeState after) throws CommitFailedException {
		// nothing to do
		recursionLevel++;
	}

	@Override
	public void leave(NodeState before, NodeState after) throws CommitFailedException {

		root.callback.indexUpdate();

		recursionLevel--;
		if (recursionLevel > 0) {
			return;
		}

		NodeBuilder data = root.definition.child(DATA_NODE_NAME);
		setPrimaryType(data);

		NodeBuilder properties = data.child("properties");
		setPrimaryType(properties);

		String[] cmsSerialized = this.propertyNameCMS.serialize();
		for (int i = 0; i < cmsSerialized.length; i++) {
			data.setProperty("property" + i, cmsSerialized[i]);
		}

		data.setProperty(PROPERTY_CMS_ROWS, propertyNameCMS.getRows());
		data.setProperty(PROPERTY_CMS_COLS, propertyNameCMS.getCols());

		for (Map.Entry<String, PropertyStatistics> e : propertyStatistics.entrySet()) {
			NodeBuilder statNode = properties.child(e.getKey());
			PropertyStatistics propStats = e.getValue();
			String name = propStats.getName();

			setPrimaryType(statNode);
			statNode.setProperty("count", propStats.getCount());

			String hllSerialized = propStats.getHll().serialize();

			CountMinSketch valueSketch = propStats.getValueSketch();
			String[] valueSketchSerialized = valueSketch.serialize();

			for (int i = 0; i < valueSketchSerialized.length; i++) {
				statNode.setProperty(VALUE_SKETCH + i, valueSketchSerialized[i]);
			}

			statNode.setProperty(VALUE_SKETCH_ROWS, (long) valueSketch.getRows(), Type.LONG);
			statNode.setProperty(VALUE_SKETCH_COLS, (long) valueSketch.getCols(), Type.LONG);

			// TODO: consider using HyperLogLog4TailCut64 so that we only store a long
			// rather than array
			statNode.setProperty(PROPERTY_HLL_NAME, hllSerialized);
			List<PropertyInfo> topKElements = propStats.getTopKValuesDescending();
			if (!topKElements.isEmpty()) {
				List<String> valueNames = getByFieldName(topKElements, PropertyInfo::getName);
				List<Long> valueCounts = getByFieldName(topKElements, PropertyInfo::getCount);
				statNode.setProperty(PROPERTY_TOPK_NAME, valueNames, Type.STRINGS);
				statNode.setProperty(PROPERTY_TOPK_COUNT, valueCounts, Type.LONGS);
			}
		}

		propertyStatistics.clear();
	}

	private <T> List<T> getByFieldName(List<PropertyInfo> propertyInfos, Function<PropertyInfo, T> field) {
		return propertyInfos.stream().map(field).collect(Collectors.toList());
	}

	private static void setPrimaryType(NodeBuilder builder) {
		builder.setProperty(JCR_PRIMARYTYPE, NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.NAME);
	}

	@Override
	public void propertyAdded(PropertyState after) throws CommitFailedException {
		propertyHasNewValue(after);
	}

	@Override
	public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
		propertyHasNewValue(after);
	}

	private void propertyHasNewValue(PropertyState after) {
		String propertyName = after.getName();
		int propertyNameHash = propertyName.hashCode();
		long properHash = Hash.hash64(propertyNameHash);
		// this shouldn't happen
		if (propertyName.equals("p0")) {
			System.out.println("ERROR");
		}
		propertyNameCMS.add(properHash);
		long approxCount = propertyNameCMS.estimateCount(properHash);
		if (approxCount >= root.commonPropertyThreshold) {
			Type<?> t = after.getType();
			if (after.isArray()) {
				Type<?> base = t.getBaseType();
				int count = after.count();
				for (int i = 0; i < count; i++) {
					updatePropertyStatistics(propertyName, after.getValue(base, i));
				}
			} else {
				updatePropertyStatistics(propertyName, after.getValue(t));
			}
		}
	}

	private void updatePropertyStatistics(String propertyName, Object val) {
		PropertyStatistics ps = propertyStatistics.get(propertyName);
		if (ps == null) {
			ps = readPropertyStatistics(propertyName);
			if (ps == null) {
				ps = new PropertyStatistics(propertyName, 0, new HyperLogLog(64), new CountMinSketch(0.01, 0.99),
						new TopKElements(new HashMap<>(), K_ELEMENTS));
			}
		}
		long hash64 = Hash.hash64((val.hashCode()));
		ps.updateHll(hash64);
		propertyStatistics.put(propertyName, ps);
		ps.updateValueCounts(val, hash64);
		ps.inc(1);
	}

	private PropertyStatistics readPropertyStatistics(String propertyName) {
		if (!root.definition.hasChildNode(DATA_NODE_NAME)) {
			return null;
		}

		NodeBuilder data = root.definition.getChildNode(DATA_NODE_NAME);
		if (!data.hasChildNode("properties")) {
			return null;
		}

		Iterable<String> childNodes = data.getChildNodeNames();
		for (String name : childNodes) {
			System.out.println(name);
		}

		NodeBuilder properties = data.getChildNode("properties");
		if (!properties.hasChildNode(propertyName)) {
			return null;
		}

		NodeBuilder prop = properties.getChildNode(propertyName);
		PropertyState count = prop.getProperty("count");
		if (count == null) {
			return null;
		}

		long c = count.getValue(Type.LONG);
		PropertyState hps = prop.getProperty("uniqueHLL");
		String hpsIndexed = hps.getValue(Type.STRING);
		byte[] hllData = HyperLogLog.deserialize(hpsIndexed);

		PropertyState topKValueNames = prop.getProperty(PROPERTY_TOPK_NAME);
		PropertyState topKValueCounts = prop.getProperty(PROPERTY_TOPK_COUNT);
		TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts);
		CountMinSketch valueSketch = readCMS(prop, VALUE_SKETCH, VALUE_SKETCH_ROWS, VALUE_SKETCH_COLS);

		return new PropertyStatistics(propertyName, c, new HyperLogLog(64, hllData), valueSketch, topKElements);
	}

	private CountMinSketch readCMS(NodeBuilder node, String cmsName, String rowName, String colName) {

		PropertyState rowsProp = node.getProperty(rowName);
		// .getValue(Type.LONG).intValue()
		PropertyState colsProp = node.getProperty(colName);
		if (rowsProp == null || colsProp == null) {
			return null;
		}

		int rows = rowsProp.getValue(Type.LONG).intValue();
		int cols = colsProp.getValue(Type.LONG).intValue();
		long[][] data = new long[rows][cols];

		for (int i = 0; i < rows; i++) {
			PropertyState ps = node.getProperty(cmsName + i);
			if (ps != null) {
				String s = ps.getValue(Type.STRING);
				try {
					long[] row = CountMinSketch.deserialize(s);
					data[i] = row;
				} catch (NumberFormatException e) {
					LOG.warn("Can not parse " + s);
				}
			}
		}

		return new CountMinSketch(rows, cols, data);
	}

	private TopKElements readTopKElements(PropertyState valueNames, PropertyState valueCounts) {
		if (valueNames != null && valueCounts != null) {
			@NotNull
			Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
			@NotNull
			Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
			Map<String, Long> valuesToCounts = TopKElements.deserialize(valueNamesIter, valueCountsIter);
			return new TopKElements(valuesToCounts, K_ELEMENTS);
		}

		return new TopKElements(new HashMap<>(), K_ELEMENTS);
	}

	@Override
	public void propertyDeleted(PropertyState before) throws CommitFailedException {
		// nothing to do
	}

	@Override
	@Nullable
	public Editor childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
		return this;
	}

	@Override
	@Nullable
	public Editor childNodeAdded(String name, NodeState after) throws CommitFailedException {
		return this;
	}

	@Override
	@Nullable
	public Editor childNodeDeleted(String name, NodeState before) throws CommitFailedException {
		return this;
	}

	public static class StatisticsRoot {
		final int resolution;
		final long seed;
		final int bitMask;
		final NodeBuilder definition;
		final NodeState root;
		final IndexUpdateCallback callback;
		private final int commonPropertyThreshold;

		StatisticsRoot(int resolution, long seed, NodeBuilder definition, NodeState root, IndexUpdateCallback callback,
				int commonPropertyThreshold) {
			this.resolution = resolution;
			this.seed = seed;
			// if resolution is 1000, then the bitMask is 1023 (bits 0..9 set)
			this.bitMask = (Integer.highestOneBit(resolution) * 2) - 1;
			this.definition = definition;
			this.root = root;
			this.callback = callback;
			this.commonPropertyThreshold = commonPropertyThreshold;
		}
	}
}
