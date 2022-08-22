package org.apache.jackrabbit.oak.plugins.index.statistics;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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
	public static final String PROPERTY_TOPK_NAME = "mostFrequentElements";

	private final CountMinSketch propertyNameCMS;
	private TopKElements topKElements;
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
			for (int i = 0; i < propertyNameCMS.getData().length; i++) {
				PropertyState ps = data.getProperty("property" + i);
				if (ps != null) {
					String s = ps.getValue(Type.STRING);
					String[] list = s.split(" ");
					for (int j = 0; j < list.length; j++) {
						try {
							long x = Long.parseLong(list[j]);
							propertyNameCMS.getData()[i][j] = x;
						} catch (NumberFormatException e) {
							LOG.warn("Can not parse " + s);
						}
					}
				}
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

		for (Map.Entry<String, PropertyStatistics> e : propertyStatistics.entrySet()) {
			NodeBuilder statNode = properties.child(e.getKey());
			PropertyStatistics propStats = e.getValue();
			String name = propStats.getName();

			setPrimaryType(statNode);
			statNode.setProperty("count", propStats.getCount());

			String hllSerialized = propStats.getHll().serialize();
			// TODO: consider using HyperLogLog4TailCut64 so that we only store a long
			// rather than array
			statNode.setProperty(PROPERTY_HLL_NAME, hllSerialized);
			String topElements = propStats.getSortedTopKElements();
			if (!topElements.isEmpty() && !topElements.equals("[]")) {
				statNode.setProperty("topKElements", topElements);
			}
			propStats.clear();
		}

		propertyStatistics.clear();
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

		PropertyState topKElementsProp = prop.getProperty("topKElements");
//		if (topKElementsProp == null) {
//			return null;
//		}

		long c = count.getValue(Type.LONG);
		PropertyState hps = prop.getProperty("uniqueHLL");
		String hpsIndexed = hps.getValue(Type.STRING);
		byte[] hllData = HyperLogLog.deserialize(hpsIndexed);

		TopKElements topKElements = null;
		if (topKElementsProp != null) {
			String topKElementsIdx = topKElementsProp.getValue(Type.STRING);
			Map<String, Long> valuesToCounts = TopKElements.deserialize(topKElementsIdx);
			topKElements = new TopKElements(valuesToCounts, K_ELEMENTS);
		} else {
			topKElements = new TopKElements(new HashMap<>(), K_ELEMENTS);
		}
//		TopKElements topKElements = new TopKElements(topKValuesIndexed, topKCountsIndexed, numCountValues);

		return new PropertyStatistics(propertyName, c, new HyperLogLog(64, hllData), new CountMinSketch(0.01, 0.99),
				topKElements);
	}

	private long length(Iterable<? extends Object> iterable) {
		long count = 0;
		for (Object o : iterable) {
			count++;
		}

		return count;
	}

	// assume that topCounts and topValues are sorted in the right order
	private Map<String, Long> getValueToCount(String[] topValues, Iterable<Long> topCounts, long numCounts) {
		Map<String, Long> valueToCounts = new HashMap<>();
		int i = 0;
		for (Long count : topCounts) {
			valueToCounts.put(topValues[i], count);
			i++;
		}
		return valueToCounts;
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
