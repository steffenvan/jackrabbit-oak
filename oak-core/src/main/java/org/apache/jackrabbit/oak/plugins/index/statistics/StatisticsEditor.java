package org.apache.jackrabbit.oak.plugins.index.statistics;

import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.counter.SipHash;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.Nullable;

public class StatisticsEditor implements Editor {

	public static final String DATA_NODE_NAME = "index";

	public static final int DEFAULT_RESOLUTION = 1000;
	public static final int DEFAULT_HLL_SIZE = 10;

	public static final String PROPERTY_CMS_NAME = "propertiesCountMinSketch";
	public static final String PROPERTY_HLL_NAME = "uniqueHLL";

	private final CountMinSketch propertyNameCMS;
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
	}

	private StatisticsEditor(StatisticsRoot root, StatisticsEditor parent, String name, SipHash hash,
			CountMinSketch propertyNameCMS, Map<String, PropertyStatistics> propertyStatistics) {
		this.parent = parent;
		this.root = root;
		this.name = name;
		this.hash = hash;
		this.propertyNameCMS = propertyNameCMS;
		this.propertyStatistics = propertyStatistics;
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
		NodeBuilder builder = data.child("properties");

		for (Map.Entry<String, PropertyStatistics> entry : propertyStatistics.entrySet()) {
			NodeBuilder statNode = builder.child(entry.getKey());

			statNode.setProperty("uniqueHLLCount", entry.getValue().getCount());
			String hllSerialized = entry.getValue().getHll().serialize();
			// TODO: consider using HyperLogLog4TailCut64 so that we only store a long
			// rather than array.
			statNode.setProperty(PROPERTY_HLL_NAME, hllSerialized);
		}
		NodeBuilder cmsNode = builder.child(PROPERTY_CMS_NAME);

		String[] cmsSerialized = this.propertyNameCMS.serialize();
		for (int i = 0; i < cmsSerialized.length; i++) {
			String row = cmsSerialized[i];
			cmsNode.setProperty("p" + i, row);
		}

		propertyStatistics.clear();
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

		if (propertyNameCMS.propertyNameIsCommon(properHash)) {
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
			// TODO: load data from previous index
			NodeBuilder data = root.definition.child(DATA_NODE_NAME);
			@Nullable
			PropertyState storedHLLProperty = data.child("properties").child(propertyName)
					.getProperty(PROPERTY_HLL_NAME);
			System.out.println(propertyName + ": " + storedHLLProperty);
			// read from data from the countProperty if it already exists in the index
			// otherwise we create a new one from scratch.
			if (storedHLLProperty == null) {
				ps = new PropertyStatistics(propertyName, 0, new HyperLogLog(DEFAULT_HLL_SIZE));
			} else {
				long storedCount = storedHLLProperty.getValue(Type.LONG);
				// TODO: Technically we should also check that
				PropertyState hps = data.child("properties").child(propertyName).getProperty(PROPERTY_HLL_NAME);
				String hpsIndexed = hps.getValue(Type.STRING);
				byte[] storedHLLData = HyperLogLog.deserialize(hpsIndexed);
				ps = new PropertyStatistics(propertyName, storedCount,
						new HyperLogLog(DEFAULT_HLL_SIZE, storedHLLData));
			}
			propertyStatistics.put(propertyName, ps);
		}
		long hash64 = Hash.hash64(val.hashCode());
		ps.updateHll(hash64);
		ps.inc(1);
	}

//	private void update(long hash64, Object val) {
//		long hash64 = Hash.hash64((val.hashCode()));
//		ps.updateHll(hash64);
//		
//	}

//	private PropertyStatistics fromPropertyState(PropertyState ps, String propertyName) {
//		if (ps == null) {
//			return new PropertyStatistics(propertyName, 0, new HyperLogLog(DEFAULT_HLL_SIZE));
//		}
//
//		long storedCount = ps.getValue(Type.LONG);
//		PropertyState hps = data.child("properties").child(propertyName).getProperty("uniqueHLL");
//		String hpsIndexed = hps.getValue(Type.STRING);
//		byte[] hllData = HyperLogLog.deserialize(hpsIndexed);
//		ps = new PropertyStatistics(propertyName, storedCount, new HyperLogLog(64, hllData));
//
//	}

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

	private Editor getChildIndexEditor(String name, SipHash hash) {
		return new StatisticsEditor(root, this, name, hash, propertyNameCMS, propertyStatistics);
	}

	public static class StatisticsRoot {
		final int resolution;
		final long seed;
		final int bitMask;
		final NodeBuilder definition;
		final NodeState root;
		final IndexUpdateCallback callback;

		StatisticsRoot(int resolution, long seed, NodeBuilder definition, NodeState root,
				IndexUpdateCallback callback) {
			this.resolution = resolution;
			this.seed = seed;
			// if resolution is 1000, then the bitMask is 1023 (bits 0..9 set)
			this.bitMask = (Integer.highestOneBit(resolution) * 2) - 1;
			this.definition = definition;
			this.root = root;
			this.callback = callback;
		}
	}
}
