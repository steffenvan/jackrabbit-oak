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
import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;

public class ContentStatistics extends AnnotatedStandardMBean implements ContentStatisticsMBean {

	private NodeStore store;
	public final static String STATISTICS_INDEX_NAME = "statistics";

	public ContentStatistics(NodeStore store) {
		super(ContentStatisticsMBean.class);
		this.store = store;
	}

	@Override
	public EstimationResult getSinglePropertyEstimation(String name) {
		// get children of statistics index
		// extract the child with the specified name.
		// deserialize the statistics information stored in the node
		// return the result

		NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
		if (statisticsDataNode == null) {
		    return null;
		}
		NodeState property = statisticsDataNode.getChildNode("properties")
				.getChildNode(name);
		if (property == null) {
		    return null;
		}
		long count = property.getProperty("count").getValue(Type.LONG);
		String storedHll = property.getProperty("uniqueHLL").getValue(Type.STRING);
		byte[] hllCounts = HyperLogLog.deserialize(storedHll);
		HyperLogLog hll = new HyperLogLog(64, hllCounts);

		return new EstimationResult(name, count, hll.estimate());
	}
	
	private NodeState getStatisticsIndexDataNodeOrNull() {
        NodeState root = store.getRoot();
        NodeState indexNode = root.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
        if (!indexNode.exists()) {
            return null;
        }
        NodeState statisticsIndexNode = indexNode.getChildNode(STATISTICS_INDEX_NAME);
        if (!statisticsIndexNode.exists()) {
            return null;
        }
        if (!"statistics".equals(statisticsIndexNode.getString("type"))) {
            return null;
        }
        NodeState statisticsDataNode = statisticsIndexNode.getChildNode(StatisticsEditor.DATA_NODE_NAME);
        if (!statisticsDataNode.exists()) {
            return null;
        }
        return statisticsDataNode;
	}

	@Override
	public long getEstimatedPropertyCount(String name) {
        NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        if (statisticsDataNode == null) {
            return -1;
        }

		@NotNull
		Iterable<? extends PropertyState> properties = statisticsDataNode.getProperties();
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
        NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        if (statisticsDataNode == null) {
            return null;
        }
        NodeState properties = statisticsDataNode.getChildNode("properties");
        if (properties == null) {
            return null;
        }
		List<EstimationResult> propertyCounts = new ArrayList<>();

		for (ChildNodeEntry child : properties.getChildNodeEntries()) {
		    String propertyName = child.getName();
			NodeState childNode = child.getNodeState();
			long count = childNode.getProperty("count").getValue(Type.LONG);
			String uniqueHll = childNode.getProperty("uniqueHLL").getValue(Type.STRING);
			byte[] hll = HyperLogLog.deserialize(uniqueHll);
			HyperLogLog hllObj = new HyperLogLog(64, hll);

			propertyCounts.add(new EstimationResult(propertyName, count, hllObj.estimate()));
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
