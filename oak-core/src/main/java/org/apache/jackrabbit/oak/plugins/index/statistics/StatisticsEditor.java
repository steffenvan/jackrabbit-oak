package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getLongOrZero;

import java.util.*;
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

	public static final Logger LOG = LoggerFactory.getLogger(StatisticsEditor.class);

	public static final String DATA_NODE_NAME = "index";

	public static final int DEFAULT_RESOLUTION = 1000;
	public static final int DEFAULT_HLL_SIZE = 64;
	public static final int K_ELEMENTS = 5;

	public static final String PROPERTY_CMS_NAME = "propertySketch";
	public static final String PROPERTY_HLL_NAME = "uniqueHLL";
	public static final String PROPERTY_TOPK_NAME = "mostFrequentValueNames";
	public static final String PROPERTY_TOPK_COUNT = "mostFrequentValueCounts";

	public static final String PROPERTY_TOP_K = "topK";
	public static final String PROPERTY_CMS_ROWS_NAME = "propertyCMSRows";
	public static final String PROPERTY_CMS_COLS_NAME = "propertyCMSCols";

	public static final String VALUE_SKETCH = "valueSketch";
	public static final String VALUE_SKETCH_ROWS = "valueSketchRows";
	public static final String VALUE_SKETCH_COLS = "valueSketchCols";

	public static final String VALUE_LENGTH_TOTAL = "valueLengthTotal";
	public final static String VALUE_LENGTH_MAX = "valueLengthMax";
	public final static String VALUE_LENGTH_MIN = "valueLengthMin";

	/* property cms parameters are not final as we read them in from the
	  NodeBuilder in the provider. We then create the sketch and pass it to this
	  constructor. On the other hand, since we don't create the value cms
	  outside this editor, we keep them final. */
	private final int valueCMSRows;
	private final int valueCMSCols;
	private CountMinSketch propertyNameCMS;
	private final Map<String, PropertyStatistics> propertyStatistics;
	private final StatisticsRoot root;
	private final StatisticsEditor parent;
	private final String name;
	private SipHash hash;
	private int recursionLevel;

	StatisticsEditor(StatisticsRoot root, CountMinSketch propertyNameCMS,
					 Map<String, PropertyStatistics> propertyStatistics, int valueCMSRows, int valueCMSCols) {
		this.root = root;
		this.name = "/";
		this.parent = null;
		this.valueCMSRows = valueCMSRows;
		this.valueCMSCols = valueCMSCols;
		this.propertyNameCMS = propertyNameCMS;
		this.propertyStatistics = propertyStatistics;
		if (root.definition.hasChildNode(DATA_NODE_NAME)) {
			NodeBuilder data = root.definition.getChildNode(DATA_NODE_NAME);
			this.propertyNameCMS = CountMinSketch.readCMS(data.getNodeState(), PROPERTY_CMS_NAME, PROPERTY_CMS_ROWS_NAME,
															  PROPERTY_CMS_COLS_NAME, LOG);
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
			data.setProperty(PROPERTY_CMS_NAME + i, cmsSerialized[i]);
		}

		data.setProperty(PROPERTY_CMS_ROWS_NAME, propertyNameCMS.getRows());
		data.setProperty(PROPERTY_CMS_COLS_NAME, propertyNameCMS.getCols());

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
			statNode.setProperty(VALUE_LENGTH_TOTAL, propStats.getValueLengthTotal(), Type.LONG);
			statNode.setProperty(VALUE_LENGTH_MAX, propStats.getValueLengthMax(), Type.LONG);
			statNode.setProperty(VALUE_LENGTH_MIN, propStats.getValueLengthMin(), Type.LONG);

			// TODO: consider using HyperLogLog4TailCut64 so that we only store a long
			// rather than array
			statNode.setProperty(PROPERTY_HLL_NAME, hllSerialized);
			List<TopKValues.ValueCountPair> topKElements = propStats.getTopKValuesDescending();
			if (!topKElements.isEmpty()) {
				List<String> valueNames = getByFieldName(topKElements, TopKValues.ValueCountPair::getValue);
				List<Long> valueCounts = getByFieldName(topKElements, TopKValues.ValueCountPair::getCount);
				statNode.setProperty(PROPERTY_TOPK_NAME, valueNames, Type.STRINGS);
				statNode.setProperty(PROPERTY_TOPK_COUNT, valueCounts, Type.LONGS);
				statNode.setProperty(PROPERTY_TOP_K, (long) valueCounts.size(), Type.LONG);
			}
		}

		propertyStatistics.clear();
	}

	private <T> List<T> getByFieldName(List<TopKValues.ValueCountPair> valueCountPairs, Function<TopKValues.ValueCountPair, T> field) {
		return valueCountPairs.stream().map(field).collect(Collectors.toList());
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
		Optional<PropertyStatistics> ps = Optional.ofNullable(
				propertyStatistics.get(propertyName));

		if (!ps.isPresent()) {
			ps = readPropertyStatistics(propertyName);
			if (!ps.isPresent()) {
				ps = Optional.of(new PropertyStatistics(propertyName, 0,
														new HyperLogLog(DEFAULT_HLL_SIZE),
														new CountMinSketch(
																valueCMSRows,
																valueCMSCols),
														new TopKValues(
																new PriorityQueue<>(),
																K_ELEMENTS,
																new HashSet<>())));
			}
		}

		String value = val.toString();
		if (value.length() > 128) {
			value = value.substring(0, 128) + " ... [" + value.hashCode() + "]";
		}

		long hash64 = Hash.hash64((value.hashCode()));
		ps.get().updateHll(hash64);
		propertyStatistics.put(propertyName, ps.get());
		ps.get().updateValueCounts(value, hash64);
		ps.get().inc(1);
	}

	private Optional<PropertyStatistics> readPropertyStatistics(String propertyName) {
		if (!root.definition.hasChildNode(DATA_NODE_NAME)) {
			return Optional.empty();
		}

		NodeBuilder data = root.definition.getChildNode(DATA_NODE_NAME);
		if (!data.hasChildNode("properties")) {
			return Optional.empty();
		}

		Iterable<String> childNodes = data.getChildNodeNames();
		for (String name : childNodes) {
			System.out.println(name);
		}

		NodeBuilder properties = data.getChildNode("properties");
		if (!properties.hasChildNode(propertyName)) {
			return Optional.empty();
		}

		NodeBuilder prop = properties.getChildNode(propertyName);
		PropertyState count = prop.getProperty("count");
		if (count == null) {
			return Optional.empty();
		}

		long c = count.getValue(Type.LONG);
		PropertyState hps = prop.getProperty("uniqueHLL");
		String hpsIndexed = hps.getValue(Type.STRING);
		byte[] hllData = HyperLogLog.deserialize(hpsIndexed);

		PropertyState topKValueNames = prop.getProperty(PROPERTY_TOPK_NAME);
		PropertyState topKValueCounts = prop.getProperty(PROPERTY_TOPK_COUNT);
		PropertyState topKNum = prop.getProperty(PROPERTY_TOP_K);
		TopKValues topKValues = readTopKElements(topKValueNames, topKValueCounts, topKNum);
		CountMinSketch valueSketch = CountMinSketch.readCMS(prop.getNodeState(), VALUE_SKETCH, VALUE_SKETCH_ROWS,
				VALUE_SKETCH_COLS, LOG);
		return Optional.of(new PropertyStatistics(propertyName, c,
												  new HyperLogLog(DEFAULT_HLL_SIZE, hllData),
												  valueSketch,
												  topKValues));
	}

	public Set<String> toSet(Iterable<String> valueNames) {
		Set<String> set = new HashSet<>();
		for (String v : valueNames) {
			set.add(v);
		}
		return set;
	}

	private TopKValues readTopKElements(PropertyState valueNames, PropertyState valueCounts, PropertyState topK) {
		if (valueNames != null && valueCounts != null) {
			@NotNull
			Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
			@NotNull
			Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
			int k = Math.toIntExact(getLongOrZero(topK));
			PriorityQueue<TopKValues.ValueCountPair> topElements = TopKValues.deserialize(valueNamesIter, valueCountsIter, k);
			Set<String> currValues = toSet(valueNamesIter);

			return new TopKValues(topElements, k, currValues);
		}

		return new TopKValues(new PriorityQueue<>(), K_ELEMENTS, new HashSet<>());
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
