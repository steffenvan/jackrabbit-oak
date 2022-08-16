package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.statistics.CountMinSketch;
import org.apache.jackrabbit.oak.plugins.index.statistics.Hash;
import org.apache.jackrabbit.oak.plugins.index.statistics.HyperLogLog;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

public class StatisticsCounter extends AnnotatedStandardMBean implements StatisticsMBean {

	private NodeStore store;
	public final static String STATISTICS_INDEX_NAME = "statistics";

	public StatisticsCounter(NodeStore store) {
		super(StatisticsMBean.class);
		this.store = store;
	}

	@Override
	public EstimationResult getSinglePropertyEstimation(String name) {
		// get children of statistics index
		// extract the child with the specified name.
		// deserialize the statistics information stored in the node
		// return the result

		NodeState root = store.getRoot();
		NodeState indexNode = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
		NodeState property = indexNode.getChildNode(STATISTICS_INDEX_NAME).getChildNode("properties")
				.getChildNode(name);
		long count = property.getProperty("count").getValue(Type.LONG);
		String storedHll = property.getProperty("uniqueHLL").getValue(Type.STRING);
		byte[] hllCounts = HyperLogLog.deserialize(storedHll);
		HyperLogLog hll = new HyperLogLog(hllCounts.length, hllCounts);

		return new EstimationResult(count, hll.estimate());
	}

	@Override
	public long getEstimatedPropertyCount(String name) {
		NodeState root = store.getRoot();
		NodeState indexNode = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
		NodeState cmsIndex = indexNode.getChildNode(STATISTICS_INDEX_NAME).getChildNode(name);

		@NotNull
		Iterable<? extends PropertyState> properties = cmsIndex.getProperties();
		int rows = getNumRows(properties);
		int cols = getNumCols(properties);

		long[][] cmsItems = new long[rows][cols];
		int i = 0;
		for (PropertyState ps : properties) {
			String storedProperty = ps.getValue(Type.STRING);
			long[] cmsRow = CountMinSketch.deserialize(storedProperty);
			cmsItems[i++] = cmsRow;
		}

		long hash = Hash.hash64(name.hashCode());

		return CountMinSketch.estimateCount(hash, cmsItems, cols, rows);

	}

	@Override
	public List<EstimationResult> getAllPropertiesEstimation() {
		// Read from the count index, deserialize and then report the estimated count of
		NodeState root = store.getRoot();
		NodeState indexNode = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
				.getChildNode(STATISTICS_INDEX_NAME).getChildNode("properties");

		List<EstimationResult> propertyCounts = new ArrayList<>();

		for (ChildNodeEntry child : indexNode.getChildNodeEntries()) {
			NodeState childNode = child.getNodeState();
			long count = childNode.getProperty("count").getValue(Type.LONG);
			String uniqueHll = childNode.getProperty("uniqueHLL").getValue(Type.STRING);
			byte[] hll = HyperLogLog.deserialize(uniqueHll);
			HyperLogLog hllObj = new HyperLogLog(hll.length, hll);

			propertyCounts.add(new EstimationResult(count, hllObj.estimate()));
		}

		return propertyCounts;

	}

	private int getNumCols(Iterable<? extends PropertyState> properties) {
		return properties.iterator().hasNext()
				? CountMinSketch.deserialize(properties.iterator().next().getValue(Type.STRING)).length
				: 0;
	}

	private int getNumRows(Iterable<? extends PropertyState> properties) {
		int count = 0;
		for (PropertyState ps : properties) {
			count++;
		}
		return count;
	}

}
