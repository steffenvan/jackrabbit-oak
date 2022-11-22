package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.statistics.*;
import org.apache.jackrabbit.oak.plugins.index.statistics.TopKValues;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getLongOrZero;
import static org.apache.jackrabbit.oak.plugins.index.statistics.PropertyReader.getStringOrEmpty;

public class ContentStatistics extends AnnotatedStandardMBean
        implements ContentStatisticsMBean {

    private final NodeStore store;

    public static final Logger CS_LOG = LoggerFactory.getLogger(
            ContentStatistics.class);
    public static final String STATISTICS_INDEX_NAME = "statistics";
    private static final String INDEX_RULES = "indexRules";
    private static final String PROPERTY_NAME = "name";
    private static final String VIRTUAL_PROPERTY_NAME = ":nodeName";
    private static final String PROPERTIES = "properties";
    private static final String EXACT_COUNT = "count";
    private static final String PROPERTY_TOP_K_NAME = "mostFrequentValueNames";
    private static final String PROPERTY_TOP_K_COUNT = "mostFrequentValueCounts";

    public ContentStatistics(NodeStore store) {
        super(ContentStatisticsMBean.class);
        this.store = store;
    }

    @Override
    public Optional<EstimationResult> getSinglePropertyEstimation(String name) {
        Optional<NodeState> statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        if (!statisticsDataNode.isPresent()) {
            return Optional.empty();
        }

        NodeState property = statisticsDataNode.get().getChildNode(PROPERTIES)
                                               .getChildNode(name);
        long count = getLongOrZero(property.getProperty(EXACT_COUNT));
        long valueLengthTotal = getLongOrZero(
                property.getProperty(StatisticsEditor.VALUE_LENGTH_TOTAL));
        long valueLengthMax = getLongOrZero(
                property.getProperty(StatisticsEditor.VALUE_LENGTH_MAX));
        long valueLengthMin = getLongOrZero(
                property.getProperty(StatisticsEditor.VALUE_LENGTH_MIN));


        String storedHll = getStringOrEmpty(
                property.getProperty(StatisticsEditor.PROPERTY_HLL_NAME));
        byte[] hllData = HyperLogLog.deserialize(storedHll);
        HyperLogLog hll = new HyperLogLog(hllData.length, hllData);

        PropertyState topKValueNames = property.getProperty(
                StatisticsEditor.PROPERTY_TOPK_NAME);
        PropertyState topKValueCounts = property.getProperty(
                StatisticsEditor.PROPERTY_TOPK_COUNT);
        long topK = getLongOrZero(
                property.getProperty(StatisticsEditor.PROPERTY_TOP_K));
        TopKValues topKValues = readTopKElements(topKValueNames,
                                                 topKValueCounts,
                                                 (int) topK);

        CountMinSketch cms = CountMinSketch.readCMS(statisticsDataNode.get(),
                                                        StatisticsEditor.PROPERTY_CMS_NAME,
                                                        StatisticsEditor.PROPERTY_CMS_ROWS_NAME,
                                                        StatisticsEditor.PROPERTY_CMS_COLS_NAME,
                                                        CS_LOG);
        long hash64 = Hash.hash64(name.hashCode());

        return Optional.of(
                new EstimationResult(name, count, cms.estimateCount(hash64),
                                     hll.estimate(), valueLengthTotal,
                                     valueLengthMax, valueLengthMin,
                                     topKValues));
    }

    private Optional<NodeState> getStatisticsIndexDataNodeOrNull() {
        NodeState indexNode = getIndexNode();

        if (!indexNode.exists()) {
            return Optional.empty();
        }
        NodeState statisticsIndexNode = indexNode.getChildNode(
                STATISTICS_INDEX_NAME);
        if (!statisticsIndexNode.exists()) {
            return Optional.empty();
        }
        if (!"statistics".equals(statisticsIndexNode.getString("type"))) {
            return Optional.empty();
        }
        NodeState statisticsDataNode = statisticsIndexNode.getChildNode(
                StatisticsEditor.DATA_NODE_NAME);
        if (!statisticsDataNode.exists()) {
            return Optional.empty();
        }
        return Optional.of(statisticsDataNode);
    }

    @Override
    public Optional<List<EstimationResult>> getAllPropertiesEstimation() {
        Optional<NodeState> statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        return statisticsDataNode.map(this::getEstimationResults);
    }

    @Override
    public Set<String> getIndexedPropertyNames() {
        NodeState indexNode = getIndexNode();
        Set<String> indexedPropertyNames = new TreeSet<>();
        for (ChildNodeEntry entry : indexNode.getChildNodeEntries()) {
            NodeState child = entry.getNodeState();
            // TODO: this will take 2 * n iterations. Should we pass the set in as a
            // reference and update it directly to reduce it to n iterations?
            indexedPropertyNames.addAll(getPropertyNamesForIndexNode(child));
        }

        return indexedPropertyNames;
    }

    @Override
    public Set<String> getPropertyNamesForSingleIndex(String name) {
        NodeState indexNode = getIndexNode();
        NodeState child = indexNode.getChildNode(name);

        return getPropertyNamesForIndexNode(child);
    }

    @Override
    public Optional<List<TopKValues.ValueCountPair>> getTopKValuesForProperty(
            String name, int k) {
        Optional<NodeState> statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        if (!statisticsDataNode.isPresent()) {
            return Optional.empty();
        }
        NodeState properties = statisticsDataNode.get().getChildNode(PROPERTIES);
        if (!properties.hasChildNode(name)) {
            return Optional.empty();
        }

        NodeState propertyNode = properties.getChildNode(name);

        PropertyState topKValueNames = propertyNode.getProperty(
                PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                PROPERTY_TOP_K_COUNT);
        TopKValues topKValues = readTopKElements(topKValueNames,
                                                 topKValueCounts, k);

        return Optional.ofNullable(topKValues.get());
    }

    @Override
    public Optional<List<TopKValues.ProportionInfo>> getValueProportionInfoForSingleProperty(
            String name) {
        Optional<NodeState> statisticsDataNode = getStatisticsIndexDataNodeOrNull();
        if (!statisticsDataNode.isPresent()) {
            return Optional.empty();
        }
        NodeState statisticsProperties = statisticsDataNode.get().getChildNode(
                PROPERTIES);

        if (!statisticsProperties.hasChildNode(name)) {
            return Optional.empty();
        }

        CountMinSketch nameSketch = CountMinSketch.readCMS(
                statisticsDataNode.get(), StatisticsEditor.PROPERTY_CMS_NAME,
                StatisticsEditor.PROPERTY_CMS_ROWS_NAME,
                StatisticsEditor.PROPERTY_CMS_COLS_NAME, CS_LOG);

        long totalCount = nameSketch.estimateCount(
                Hash.hash64(name.hashCode()));

        NodeState propertyNode = statisticsProperties.getChildNode(name);

        PropertyState topKValueNames = propertyNode.getProperty(
                PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                PROPERTY_TOP_K_COUNT);
        int k = Math.toIntExact(getLongOrZero(propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K)));

        TopKValues topKValues = readTopKElements(topKValueNames,
                                                 topKValueCounts, k);

        List<TopKValues.ValueCountPair> topK = topKValues.get();
        List<TopKValues.ProportionInfo> proportionInfo = new ArrayList<>();
        for (TopKValues.ValueCountPair pi : topK) {
            TopKValues.ProportionInfo dpi = new TopKValues.ProportionInfo(pi.getValue(),
                                                                          pi.getCount(),
                                                                          totalCount);
            proportionInfo.add(dpi);
        }

        long topKTotalCount = proportionInfo.stream()
                                             .mapToLong(
                                                     TopKValues.ProportionInfo::getCount)
                                             .sum();
        TopKValues.ProportionInfo totalInfo = new TopKValues.ProportionInfo("TopKCount",
                                                                            topKTotalCount,
                                                                            totalCount);
        proportionInfo.add(totalInfo);

        return Optional.of(proportionInfo);
    }

    private EstimationResult getSingleEstimationResult(String name,
                                                       NodeState indexNode) {
        NodeState properties = indexNode.getChildNode(PROPERTIES);
        NodeState propertyNode = properties.getChildNode(name);

        long count = getLongOrZero(propertyNode.getProperty(EXACT_COUNT));
        String uniqueHll = getStringOrEmpty(
                propertyNode.getProperty(StatisticsEditor.PROPERTY_HLL_NAME));

        long valueLengthTotal = getLongOrZero(
                propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_TOTAL));
        long valueLengthMax = getLongOrZero(
                propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_MAX));
        long valueLengthMin = getLongOrZero(
                propertyNode.getProperty(StatisticsEditor.VALUE_LENGTH_MIN));

        CountMinSketch cms = CountMinSketch.readCMS(indexNode,
                                                        StatisticsEditor.PROPERTY_CMS_NAME,
                                                        StatisticsEditor.PROPERTY_CMS_ROWS_NAME,
                                                        StatisticsEditor.PROPERTY_CMS_COLS_NAME,
                                                        CS_LOG);

        byte[] hllData = HyperLogLog.deserialize(uniqueHll);
        HyperLogLog hll = new HyperLogLog(hllData.length, hllData);

        PropertyState topKValueNames = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOPK_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOPK_COUNT);
        PropertyState topK = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K);
        TopKValues topKValues = readTopKElements(topKValueNames,
                                                 topKValueCounts, topK);

        long hash64 = Hash.hash64(name.hashCode());

        return new EstimationResult(name, count, cms.estimateCount(hash64),
                                    hll.estimate(), valueLengthTotal,
                                    valueLengthMax, valueLengthMin,
                                    topKValues);
    }

    private List<EstimationResult> getEstimationResults(
            NodeState indexNode) {
        List<EstimationResult> propertyCounts = new ArrayList<>();

        for (ChildNodeEntry child : indexNode.getChildNode(PROPERTIES)
                                             .getChildNodeEntries()) {
            propertyCounts.add(
                    getSingleEstimationResult(child.getName(), indexNode));
        }

        return propertyCounts.stream()
                             .sorted(Comparator.comparing(
                                                       EstimationResult::getCount)
                                               .reversed()
                                               .thenComparingLong(
                                                       EstimationResult::getHllCount))
                             .collect(Collectors.toList());
    }

    private TopKValues readTopKElements(PropertyState valueNames,
                                        PropertyState valueCounts,
                                        PropertyState topK) {
        if (valueNames != null && valueCounts != null) {
            @NotNull
            Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
            @NotNull
            Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
            int k = Math.toIntExact(topK.getValue(Type.LONG));
            PriorityQueue<TopKValues.ValueCountPair> topValues = TopKValues.deserialize(
                    valueNamesIter, valueCountsIter, k);
            Set<String> currValues = toSet(valueNamesIter);
            return new TopKValues(topValues, k, currValues);
        }

        return new TopKValues(new PriorityQueue<>(),
                              StatisticsEditor.K_ELEMENTS, new HashSet<>());
    }

    private Set<String> toSet(Iterable<String> valueNamesIter) {
        Set<String> currValues = new HashSet<>();
        for (String v : valueNamesIter) {
            currValues.add(v);
        }
        return currValues;
    }

    private TopKValues readTopKElements(PropertyState valueNames,
                                        PropertyState valueCounts, int k) {
        if (valueNames != null && valueCounts != null) {
            @NotNull
            Iterable<String> valueNamesIter = valueNames.getValue(Type.STRINGS);
            @NotNull
            Iterable<Long> valueCountsIter = valueCounts.getValue(Type.LONGS);
            PriorityQueue<TopKValues.ValueCountPair> topValues = TopKValues.deserialize(
                    valueNamesIter, valueCountsIter, k);
            Set<String> currValues = toSet(valueNamesIter);
            return new TopKValues(topValues, k, currValues);
        }

        return new TopKValues(new PriorityQueue<>(), k, new HashSet<>());
    }

    /**
     * To find the property names of an index, we traverse the index node's
     * children until we find nodes that are "valid". A node is considered a
     * valid property, if it fulfills certain criteria. Like that it needs to
     * have a "name" property that does not start with "function" and more. See
     * {@link #hasValidPropertyNameNode(NodeState)} and
     * {@link #isValidPropertyName(String)}
     *
     * @param nodeState - the node state of the provided index node
     * @return the properties of the provided index node.
     */
    public Set<String> getPropertyNamesForIndexNode(NodeState nodeState) {
        Set<String> propStates = new TreeSet<>();
        if (nodeState.hasChildNode(INDEX_RULES)) {
            NodeState propertiesChildOfIndexNode = getPropertiesNode(
                    nodeState.getChildNode(INDEX_RULES));
            if (propertiesChildOfIndexNode.exists()) {
                for (ChildNodeEntry ce : propertiesChildOfIndexNode.getChildNodeEntries()) {
                    NodeState childNode = ce.getNodeState();
                    if (hasValidPropertyNameNode(childNode)) {
                        String propertyName;
                        // we assume that this (property) node either has a property named: "name" or "properties"
                        if (childNode.hasProperty(PROPERTY_NAME)) {
                            propertyName = parse(
                                    getStringOrEmpty(childNode.getProperty(
                                            PROPERTY_NAME)));
                        } else {
                            propertyName = parse(
                                    getStringOrEmpty(
                                            childNode.getProperty(PROPERTIES)));
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
        return !propertyName.startsWith("function") &&
                !propertyName.equals("jcr:path") &&
                !propertyName.equals("rep:facet");
    }

    private NodeState getIndexNode() {
        return store.getRoot()
                    .getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
    }

    private boolean isRegExp(NodeState nodeState) {
        PropertyState regExp = nodeState.getProperty("isRegexp");
        return regExp != null && regExp.getValue(Type.BOOLEAN);
    }

    private boolean hasValidPropertyNameNode(NodeState nodeState) {
        return nodeState.exists() && (nodeState.hasProperty(PROPERTY_NAME) ||
                nodeState.hasProperty("properties"))
                && !isRegExp(nodeState) && !hasVirtualProperty(nodeState);
    }

    private boolean hasVirtualProperty(NodeState nodeState) {
        return nodeState.hasProperty(PROPERTY_NAME)
                && nodeState.getProperty(PROPERTY_NAME).getValue(Type.STRING)
                            .equals(VIRTUAL_PROPERTY_NAME)
                || nodeState.hasProperty("properties")
                && nodeState.getProperty("properties").getValue(Type.STRING)
                            .equals(VIRTUAL_PROPERTY_NAME);
    }

    /*
     starting from the "indexRule" node, we perform a simple DFS to find the
     corresponding properties node of the index node.
     */
    private NodeState getPropertiesNode(NodeState nodeState) {
        if (nodeState == null || !nodeState.exists()) {
            return EmptyNodeState.MISSING_NODE;
        }

        if (nodeState.hasChildNode(PROPERTIES)) {
            return nodeState.getChildNode(PROPERTIES);
        }

        for (ChildNodeEntry c : nodeState.getChildNodeEntries()) {
            NodeState ns = getPropertiesNode(c.getNodeState());
            if (ns.exists()) {
                return ns;
            }
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
               ? CountMinSketch.deserialize(properties.iterator().next()
                                                      .getValue(
                                                              Type.STRING)).length
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
