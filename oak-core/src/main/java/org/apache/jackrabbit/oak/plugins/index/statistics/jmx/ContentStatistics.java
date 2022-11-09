package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.statistics.CountMinSketch;
import org.apache.jackrabbit.oak.plugins.index.statistics.Hash;
import org.apache.jackrabbit.oak.plugins.index.statistics.HyperLogLog;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKPropertyInfo;
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

import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getLongOrZero;
import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getStringOrEmpty;

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
        long count = getLongOrZero(property.getProperty("count"));
        long valueLengthTotal = getLongOrZero(property.getProperty(StatisticsEditor.VALUE_LENGTH_TOTAL));
        long valueLengthMax = getLongOrZero(property.getProperty(StatisticsEditor.VALUE_LENGTH_MAX));
        long valueLengthMin = getLongOrZero(property.getProperty(StatisticsEditor.VALUE_LENGTH_MIN));

        CountMinSketch cms = PropertyStatistics.readCMS(statisticsDataNode, StatisticsEditor.PROPERTY_CMS_NAME, StatisticsEditor.PROPERTY_CMS_ROWS_NAME, StatisticsEditor.PROPERTY_CMS_COLS_NAME, CS_LOG);
        long hash64 = Hash.hash64(name.hashCode());

        String storedHll = getStringOrEmpty(property.getProperty("uniqueHLL"));
        byte[] hllCounts = HyperLogLog.deserialize(storedHll);
        HyperLogLog hll = new HyperLogLog(64, hllCounts);
        PropertyState topKValueNames = property.getProperty(StatisticsEditor.PROPERTY_TOPK_NAME);
        PropertyState topKValueCounts = property.getProperty(StatisticsEditor.PROPERTY_TOPK_COUNT);
        long topK = getLongOrZero(property.getProperty(StatisticsEditor.PROPERTY_TOP_K));
        TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts, (int) topK);

        return new EstimationResult(name, count, cms.estimateCount(hash64), hll.estimate(), valueLengthTotal, valueLengthMax, valueLengthMin, topKElements.toString());
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

//        NodeState properties = statisticsDataNode.getChildNode(PROPERTIES);

        return getEstimationResults(statisticsDataNode);

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
    public List<TopKPropertyInfo> getTopKIndexedPropertiesForSingleProperty(String name, int k) {
        NodeState statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        assert statisticsDataNode != null;
        NodeState properties = statisticsDataNode.getChildNode(PROPERTIES);
        if (!properties.hasChildNode(name)) {
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
        if (!statisticsProperties.hasChildNode(name)) {
            return null;
        }

        CountMinSketch nameSketch = PropertyStatistics.readCMS(statisticsDataNode, StatisticsEditor.PROPERTY_CMS_NAME,
                                                               StatisticsEditor.PROPERTY_CMS_ROWS_NAME, StatisticsEditor.PROPERTY_CMS_COLS_NAME, CS_LOG);

        long totalCount = nameSketch.estimateCount(Hash.hash64(name.hashCode()));

        NodeState propertyNode = statisticsProperties.getChildNode(name);

        PropertyState topKValueNames = propertyNode.getProperty(PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(PROPERTY_TOP_K_COUNT);
        PropertyState kProp = propertyNode.getProperty(StatisticsEditor.PROPERTY_TOP_K);
        int k = Math.toIntExact(kProp.getValue(Type.LONG));

        TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts, k);

        List<TopKPropertyInfo> topK = topKElements.get();
        List<DetailedPropertyInfo> dpis = new ArrayList<>();
        for (TopKPropertyInfo pi : topK) {
            DetailedPropertyInfo dpi = new DetailedPropertyInfo(pi.getName(), pi.getCount(), totalCount);
            dpis.add(dpi);
        }

        long topKTotalCount = dpis.stream().mapToLong(DetailedPropertyInfo::getCount).sum();
        DetailedPropertyInfo totalInfo = new DetailedPropertyInfo("TopKCount", topKTotalCount, totalCount);
        dpis.add(totalInfo);

        return dpis;
    }

    private EstimationResult getSingleEstimationResult(String name, NodeState indexNode) {
        NodeState properties = indexNode.getChildNode(PROPERTIES);
        NodeState propertyNode = properties.getChildNode(name);

        long count = propertyNode.getProperty("count").getValue(Type.LONG);
        String uniqueHll = propertyNode.getProperty("uniqueHLL").getValue(Type.STRING);

        long valueLengthTotal = getLongOrZero(propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_TOTAL));
        long valueLengthMax = getLongOrZero(propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_MAX));
        long valueLengthMin = getLongOrZero(propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_MIN));

        // TODO: read the first parameter from the node itself as well rather than
        // hard-coding 64
        CountMinSketch cms = PropertyStatistics.readCMS(indexNode, StatisticsEditor.PROPERTY_CMS_NAME, StatisticsEditor.PROPERTY_CMS_ROWS_NAME, StatisticsEditor.PROPERTY_CMS_COLS_NAME, CS_LOG);
        long hash64 = Hash.hash64(name.hashCode());

        byte[] hll = HyperLogLog.deserialize(uniqueHll);
        HyperLogLog hllObj = new HyperLogLog(64, hll);
        PropertyState topKValueNames = propertyNode.getProperty(StatisticsEditor.PROPERTY_TOPK_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(StatisticsEditor.PROPERTY_TOPK_COUNT);
        PropertyState topK = propertyNode.getProperty(StatisticsEditor.PROPERTY_TOP_K);
        TopKElements topKElements = readTopKElements(topKValueNames, topKValueCounts, topK);

        return new EstimationResult(name, count, cms.estimateCount(hash64), hllObj.estimate(), valueLengthTotal, valueLengthMax, valueLengthMin, topKElements.toString());
    }

    private List<EstimationResult> getEstimationResults(NodeState indexNode) {
        List<EstimationResult> propertyCounts = new ArrayList<>();

        for (ChildNodeEntry child : indexNode.getChildNode(PROPERTIES).getChildNodeEntries()) {
            propertyCounts.add(getSingleEstimationResult(child.getName(), indexNode));
        }
        return propertyCounts.stream().sorted(Comparator.comparing(EstimationResult::getCount).reversed().thenComparingLong(EstimationResult::getHllCount)).collect(Collectors.toList());

//        return propertyCounts.stream().sorted(Comparator.comparing(EstimationResult::getCount).reversed()).filter(x -> x.getHllCount() < 5).collect(Collectors.toList());
    }

    // TODO: create method for getting the estimation of the value themselves.
    // jcr:primaryType for instance.
    // if it's part of the top K, return
    // otherwie use cms to estimate.

    private TopKElements readTopKElements(PropertyState valueNames, PropertyState valueCounts, PropertyState topK) {
        int k = Math.toIntExact(topK.getValue(Type.LONG));
        if (valueNames != null && valueCounts != null) {
            @NotNull
            Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
            @NotNull
            Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
            PriorityQueue<TopKElements.ValueCountPair> topValues = TopKElements.deserialize(valueNamesIter, valueCountsIter, k);
            Set<String> currValues = toSet(valueNamesIter);
            return new TopKElements(topValues, k, currValues);
        }

        return new TopKElements(new PriorityQueue<>(), k, new HashSet<>());
    }

    private Set<String> toSet(Iterable<String> valueNamesIter) {
        Set<String> currValues = new HashSet<>();
        for (String v : valueNamesIter) {
            currValues.add(v);
        }
        return currValues;
    }

    private TopKElements readTopKElements(PropertyState valueNames, PropertyState valueCounts, int k) {
        if (valueNames != null && valueCounts != null) {
            @NotNull
            Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
            @NotNull
            Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
            PriorityQueue<TopKElements.ValueCountPair> topValues = TopKElements.deserialize(valueNamesIter, valueCountsIter, k);
            Set<String> currValues = toSet(valueNamesIter);
            return new TopKElements(topValues, k, currValues);
        }

        return new TopKElements(new PriorityQueue<>(), k, new HashSet<>());
    }

    public Set<String> getIndexedProperties(NodeState nodeState) throws IllegalArgumentException {
        Set<String> propStates = new TreeSet<>();
        if (nodeState.hasChildNode(INDEX_RULES)) {
            NodeState propertyNode = getPropertiesNode(nodeState.getChildNode(INDEX_RULES));
            if (propertyNode.exists()) {
                for (ChildNodeEntry ce : propertyNode.getChildNodeEntries()) {
                    NodeState childNode = ce.getNodeState();
                    if (hasValidPropertyNameNode(childNode)) {
                        String propertyName;
                        // we assume that this node either has a property named: "name" or "properties"
                        if (childNode.hasProperty(PROPERTY_NAME)) {
                            propertyName = parse(childNode.getProperty(PROPERTY_NAME).getValue(Type.STRING));
                        } else {
                            propertyName = parse(childNode.getProperty("properties").getValue(Type.STRING));
                        }
                        if (isValidPropertyName(propertyName)) {
                            propStates.add(propertyName);
                        }
                    }
                }
            }
        }
        return propStates;
    }

    private boolean isValidPropertyName(String propertyName) {
        return !propertyName.startsWith("function") && !propertyName.equals("jcr:path") && !propertyName.equals("rep:facet");
    }

    private NodeState getIndexNode() {
        return store.getRoot().getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
    }

    private boolean isRegExp(NodeState nodeState) {
        PropertyState regExp = nodeState.getProperty("isRegexp");
        return regExp != null && regExp.getValue(Type.BOOLEAN);
    }

    private boolean hasValidPropertyNameNode(NodeState nodeState) {
        return nodeState.exists() && (nodeState.hasProperty(PROPERTY_NAME) || nodeState.hasProperty("properties"))
                && !isRegExp(nodeState) && !hasVirtualProperty(nodeState);
    }

    private boolean hasVirtualProperty(NodeState nodeState) {
        return nodeState.hasProperty(PROPERTY_NAME)
                && nodeState.getProperty(PROPERTY_NAME).getValue(Type.STRING).equals(VIRTUAL_PROPERTY_NAME)
                || nodeState.hasProperty("properties")
                        && nodeState.getProperty("properties").getValue(Type.STRING).equals(VIRTUAL_PROPERTY_NAME);
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

    private static String parse(String propertyName) {
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
