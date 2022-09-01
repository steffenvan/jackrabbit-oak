package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.statistics.CountMinSketch;
import org.apache.jackrabbit.oak.plugins.index.statistics.Hash;
import org.apache.jackrabbit.oak.plugins.index.statistics.HyperLogLog;
import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyInfo;
import org.apache.jackrabbit.oak.plugins.index.statistics.PropertyStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKElements;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentStatistics extends AnnotatedStandardMBean implements ContentStatisticsMBean {

	private NodeStore store;

	public static final Logger CS_LOG = LoggerFactory.getLogger(ContentStatistics.class);
	public static final String STATISTICS_INDEX_NAME = "statistics";
	private static final String INDEX_RULES = "indexRules";
	private static final String PROPERTY_NAME = "name";
	private static final String VIRTUAL_PROPERTY_NAME = ":nodeName";
	private static final String PROPERTIES = "properties";
	private static final String PROPERTY_TOP_K_NAME = "mostFrequentValueNames";
	private static final String PROPERTY_TOP_K_COUNT = "mostFrequentValueCounts";

	public ContentStatistics(NodeStore store) {
		super(ContentStatisticsMBean.class);
		this.store = store;
	}

	@Override
	public EstimationResult getSinglePropertyEstimation(String name) {
		NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
		if (statisticsDataNode == null) {
			return null;
		}
		NodeState property = statisticsDataNode.getChildNode(PROPERTIES).getChildNode(name);
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
		NodeState indexNode = getIndexNode();

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
		NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
		if (statisticsDataNode == null) {
			return null;
		}

		NodeState properties = statisticsDataNode.getChildNode(PROPERTIES);
		if (properties == null) {
			return null;
		}

		return getEstimationResults(properties);

	}

	@Override
	public Set<String> getIndexedPropertyNames() {
		NodeState indexNode = getIndexNode();
		Set<String> indexedPropertyNames = new TreeSet<>();
		for (ChildNodeEntry entry : indexNode.getChildNodeEntries()) {
			NodeState child = entry.getNodeState();
			// TODO: this will take 2 * n iterations. Should we pass the set in as a
			// reference and update it directly to reduce it to n iterations?
			indexedPropertyNames.addAll(getIndexedProperties(child));
		}

		return indexedPropertyNames;
	}

	@Override
	public Set<String> getIndexedPropertyNamesForSingleIndex(String name) {
		NodeState indexNode = getIndexNode();
		NodeState child = indexNode.getChildNode(name);

		return getIndexedProperties(child);
	}

	@Override
	public List<PropertyInfo> getTopKIndexedPropertiesForSingleProperty(String name, int k) {
		NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
		NodeState properties = statisticsDataNode.getChildNode(PROPERTIES);
		if (properties == null || !properties.hasChildNode(name)) {
			return null;
		}

		NodeState propertyNode = properties.getChildNode(name);

		PropertyState topKValueNames = propertyNode.getProperty(PROPERTY_TOP_K_NAME);
		PropertyState topKValueCounts = propertyNode.getProperty(PROPERTY_TOP_K_COUNT);
		TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts, k);

		return topKElements.get();
	}

	@Override
	public List<DetailedPropertyInfo> getPropertyInfoForSingleProperty(String name) {
		NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
		NodeState statisticsProperties = statisticsDataNode.getChildNode(PROPERTIES);
		if (statisticsProperties == null || !statisticsProperties.hasChildNode(name)) {
			return null;
		}

		CountMinSketch nameSketch = PropertyStatistics.readCMS(statisticsDataNode, StatisticsEditor.PROPERTY_CMS_NAME,
				StatisticsEditor.PROPERTY_CMS_ROWS, StatisticsEditor.PROPERTY_CMS_COLS, CS_LOG);

		long totalCount = nameSketch.estimateCount(Hash.hash64(name.hashCode()));

		NodeState propertyNode = statisticsProperties.getChildNode(name);

		PropertyState topKValueNames = propertyNode.getProperty(PROPERTY_TOP_K_NAME);
		PropertyState topKValueCounts = propertyNode.getProperty(PROPERTY_TOP_K_COUNT);
		TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts, 5);

		List<PropertyInfo> topK = topKElements.get();
		List<DetailedPropertyInfo> dpis = new ArrayList<>();
		for (PropertyInfo pi : topK) {
			DetailedPropertyInfo dpi = new DetailedPropertyInfo(pi.getName(), pi.getCount(), totalCount);
			dpis.add(dpi);
		}

		long topKTotalCount = dpis.stream().mapToLong(x -> x.getCount()).sum();
		DetailedPropertyInfo totalInfo = new DetailedPropertyInfo("TopKCount", topKTotalCount, totalCount);
		dpis.add(totalInfo);

		return dpis;
	}

	private EstimationResult getSingleEstimationResult(String name, NodeState properties) {
		NodeState child = properties.getChildNode(name);
		long count = child.getProperty("count").getValue(Type.LONG);
		String uniqueHll = child.getProperty("uniqueHLL").getValue(Type.STRING);
		byte[] hll = HyperLogLog.deserialize(uniqueHll);
		HyperLogLog hllObj = new HyperLogLog(64, hll);
		return new EstimationResult(name, count, hllObj.estimate());
	}

	private List<EstimationResult> getEstimationResults(NodeState properties) {
		List<EstimationResult> propertyCounts = new ArrayList<>();

		for (ChildNodeEntry child : properties.getChildNodeEntries()) {
			propertyCounts.add(getSingleEstimationResult(child.getName(), properties));
		}
		return propertyCounts;
	}

	// TODO: create method for getting the estimation of the value themselves.
	// jcr:primaryType for instance.
	// if it's part of the top K, return
	// otherwie use cms to estimate.

	private TopKElements readTopKElements(PropertyState valueNames, PropertyState valueCounts, int k) {
		if (valueNames != null && valueCounts != null) {
			@NotNull
			Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
			@NotNull
			Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
			Map<String, Long> valuesToCounts = TopKElements.deserialize(valueNamesIter, valueCountsIter);
			return new TopKElements(valuesToCounts, k);
		}

		return new TopKElements(new HashMap<>(), k);
	}

	private Set<String> getIndexedProperties(NodeState nodeState) {
		Set<String> propStates = new TreeSet<>();
		if (nodeState.hasChildNode(INDEX_RULES)) {
			NodeState propertyNode = getPropertiesNode(nodeState.getChildNode(INDEX_RULES));
			if (propertyNode.exists()) {
				for (ChildNodeEntry ce : propertyNode.getChildNodeEntries()) {
					NodeState childNode = ce.getNodeState();
					if (hasValidPropertyNameNode(childNode)) {
						String propertyName = parse(childNode.getProperty(PROPERTY_NAME).getValue(Type.STRING));
						propStates.add(propertyName);
					}
				}
			}
		}
		return propStates;
	}

	private NodeState getIndexNode() {
		return store.getRoot().getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
	}

	private boolean isRegExp(NodeState nodeState) {
		PropertyState regExp = nodeState.getProperty("isRegexp");
		return regExp != null && regExp.getValue(Type.BOOLEAN);
	}

	private boolean hasValidPropertyNameNode(NodeState nodeState) {
		return nodeState.exists() && nodeState.hasProperty(PROPERTY_NAME) && !isRegExp(nodeState)
				&& !nodeState.getProperty(PROPERTY_NAME).getValue(Type.STRING).equals(VIRTUAL_PROPERTY_NAME);
	}

	// start with indexRules node
	private NodeState getPropertiesNode(NodeState nodeState) {
		if (nodeState == null || !nodeState.exists()) {
			return EmptyNodeState.MISSING_NODE;
		}

		if (nodeState.hasChildNode(PROPERTIES)) {
			return nodeState.getChildNode(PROPERTIES);
		}

		for (ChildNodeEntry c : nodeState.getChildNodeEntries()) {
			return getPropertiesNode(c.getNodeState());
		}

		return EmptyNodeState.MISSING_NODE;
	}

	private String parse(String propertyName) {
		if (propertyName.contains("/")) {
			String[] parts = propertyName.split("/");
			return parts[parts.length - 1];
		}
		return propertyName;
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
