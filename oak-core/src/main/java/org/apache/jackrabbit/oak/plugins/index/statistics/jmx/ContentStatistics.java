package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.plugins.index.statistics.*;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.jackrabbit.oak.plugins.index.statistics.StateReader.*;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor.PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor.PROPERTY_TOP_K_NAME;

public class ContentStatistics extends AnnotatedStandardMBean implements ContentStatisticsMBean {

    public static final Logger CS_LOG = LoggerFactory.getLogger(
            ContentStatistics.class);
    public static final String STATISTICS_INDEX_NAME = "statistics";
    private static final String INDEX_RULES = "indexRules";
    private static final String PROPERTY_NAME = "name";
    private static final String VIRTUAL_PROPERTY_NAME = ":nodeName";
    private final NodeStore store;

    public ContentStatistics(NodeStore store) {
        super(ContentStatisticsMBean.class);
        this.store = store;
    }

    private static String parse(String propertyName) {
        if (propertyName.contains("/")) {
            String[] parts = propertyName.split("/");
            return parts[parts.length - 1];
        }
        return propertyName;
    }

    @Override
    public Optional<EstimationResult> getSinglePropertyEstimation(String name) {
        Optional<NodeState> statisticsDataNode =
                getStatisticsIndexDataNodeOrNull(
                getIndexNode(store));

        if (!statisticsDataNode.isPresent()) {
            return Optional.empty();
        }

        NodeState property = statisticsDataNode.get()
                                               .getChildNode(PROPERTIES)
                                               .getChildNode(name);
        long count = getLongOrZero(
                property.getProperty(StatisticsEditor.EXACT_COUNT));
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
                PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = property.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_COUNT);
        long topK = getLongOrZero(
                property.getProperty(StatisticsEditor.PROPERTY_TOP_K));
        TopKValues topKValues = StatisticsEditor.readTopKElements(
                topKValueNames, topKValueCounts, (int) topK);

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
                                     topKValues.get()));
    }

    @Override
    public Optional<List<EstimationResult>> getAllPropertiesEstimation() {
        Optional<NodeState> statisticsDataNode =
                getStatisticsIndexDataNodeOrNull(
                getIndexNode(store));
        return statisticsDataNode.map(this::getEstimationResults);
    }

    @Override
    public Set<String> getIndexedPropertyNames() {
        NodeState indexNode = StateReader.getIndexNode(store);
        Set<String> indexedPropertyNames = new TreeSet<>();
        for (ChildNodeEntry entry : indexNode.getChildNodeEntries()) {
            NodeState child = entry.getNodeState();
            // TODO: this will take 2 * n iterations. Should we pass the set
            //  in as a
            // reference and update it directly to reduce it to n iterations?
            indexedPropertyNames.addAll(getPropertyNamesForIndexNode(child));
        }

        return indexedPropertyNames;
    }

    @Override
    public Set<String> getPropertyNamesForSingleIndex(String name) {
        NodeState indexNode = StateReader.getIndexNode(store);
        NodeState child = indexNode.getChildNode(name);

        return getPropertyNamesForIndexNode(child);
    }

    @Override
    public Optional<List<TopKValues.ValueCountPair>> getTopKValuesForProperty(
            String name, int k) {
        Optional<NodeState> statisticsDataNode =
                getStatisticsIndexDataNodeOrNull(
                getIndexNode(store));
        if (!statisticsDataNode.isPresent()) {
            return Optional.empty();
        }
        
        NodeState properties = statisticsDataNode.get()
                                                 .getChildNode(PROPERTIES);
        if (!properties.hasChildNode(name)) {
            return Optional.empty();
        }

        NodeState propertyNode = properties.getChildNode(name);

        PropertyState topKValueNames = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_COUNT);
        TopKValues topKValues = StatisticsEditor.readTopKElements(
                topKValueNames, topKValueCounts, k);

        return Optional.ofNullable(topKValues.get());
    }

    private List<TopKValues.ProportionInfo> getProportionalInfoForSingleProperty(
            String propertyName) {
        Optional<NodeState> statisticsDataNode =
                getStatisticsIndexDataNodeOrNull(
                getIndexNode(store));
        if (!statisticsDataNode.isPresent()) {
            return Collections.emptyList();
        }
        NodeState statisticsProperties = statisticsDataNode.get()
                                                           .getChildNode(
                                                                   PROPERTIES);

        if (!statisticsProperties.hasChildNode(propertyName)) {
            return Collections.emptyList();
        }

        CountMinSketch nameSketch = CountMinSketch.readCMS(
                statisticsDataNode.get(), StatisticsEditor.PROPERTY_CMS_NAME,
                StatisticsEditor.PROPERTY_CMS_ROWS_NAME,
                StatisticsEditor.PROPERTY_CMS_COLS_NAME, CS_LOG);

        long totalCount = nameSketch.estimateCount(
                Hash.hash64(propertyName.hashCode()));

        NodeState propertyNode = statisticsProperties.getChildNode(
                propertyName);

        PropertyState topKValueNames = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_COUNT);
        int k = Math.toIntExact(getLongOrZero(
                propertyNode.getProperty(StatisticsEditor.PROPERTY_TOP_K)));

        TopKValues topKValues = StatisticsEditor.readTopKElements(
                topKValueNames, topKValueCounts, k);
        List<TopKValues.ValueCountPair> topK = topKValues.get();
        List<TopKValues.ProportionInfo> proportionInfo = new ArrayList<>();
        for (TopKValues.ValueCountPair pi : topK) {
            TopKValues.ProportionInfo dpi = new TopKValues.ProportionInfo(
                    pi.getValue(), pi.getCount(), totalCount);
            proportionInfo.add(dpi);
        }

        long topKTotalCount = proportionInfo.stream()
                                            .mapToLong(
                                                    TopKValues.ProportionInfo::getCount)
                                            .sum();
        TopKValues.ProportionInfo totalInfo = new TopKValues.ProportionInfo(
                "TopKCount", topKTotalCount, totalCount);
        proportionInfo.add(totalInfo);

        return proportionInfo;
    }

    @Override
    public List<TopKValues.ProportionInfo> getValueProportionInfoForSingleProperty(
            String propertyName) {
        return getProportionalInfoForSingleProperty(propertyName);
    }

    private EstimationResult getSingleEstimationResult(String name,
                                                       NodeState indexNode) {
        NodeState properties = indexNode.getChildNode(PROPERTIES);
        NodeState propertyNode = properties.getChildNode(name);

        long count = getLongOrZero(
                propertyNode.getProperty(StatisticsEditor.EXACT_COUNT));

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

        String storedHll = getStringOrEmpty(
                propertyNode.getProperty(StatisticsEditor.PROPERTY_HLL_NAME));
        byte[] hllData = HyperLogLog.deserialize(storedHll);
        HyperLogLog hll = new HyperLogLog(hllData.length, hllData);

        PropertyState topKValueNames = propertyNode.getProperty(
                PROPERTY_TOP_K_NAME);
        PropertyState topKValueCounts = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_COUNT);
        PropertyState topK = propertyNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K);
        TopKValues topKValues = StatisticsEditor.readTopKElements(
                topKValueNames, topKValueCounts,
                Math.toIntExact(getLongOrZero(topK)));

        long hash64 = Hash.hash64(name.hashCode());

        return new EstimationResult(name, count, cms.estimateCount(hash64),
                                    hll.estimate(), valueLengthTotal,
                                    valueLengthMax, valueLengthMin,
                                    topKValues.get());
    }

    private List<EstimationResult> getEstimationResults(NodeState indexNode) {
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

    @Override
    public List<List<TopKValues.ProportionInfo>> getProportionInfoForIndexedProperties() {
        NodeState indexNode = StateReader.getIndexNode(store);
        List<List<TopKValues.ProportionInfo>> propInfo = new ArrayList<>();

        for (ChildNodeEntry child : indexNode.getChildNode(PROPERTIES)
                                             .getChildNodeEntries()) {
            propInfo.add(getProportionalInfoForSingleProperty(child.getName()));
        }
        return propInfo;
    }

    /**
     * To find the property names of an index, we traverse the index node's
     * children until we find nodes that are "valid". A property node is
     * considered valid, if it fulfills certain criteria. E.g. it has a "name"
     * property that does not start with "function". See:
     * <p>
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
                for (ChildNodeEntry ce :
                        propertiesChildOfIndexNode.getChildNodeEntries()) {
                    NodeState childNode = ce.getNodeState();
                    if (hasValidPropertyNameNode(childNode)) {
                        String propertyName;
                        // we assume that this (property) node either has a
                        // property named: "name" or "properties"
                        if (childNode.hasProperty(PROPERTY_NAME)) {
                            propertyName = parse(getStringOrEmpty(
                                    childNode.getProperty(PROPERTY_NAME)));
                        } else {
                            propertyName = parse(getStringOrEmpty(
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
        return !propertyName.startsWith("function") && !propertyName.equals(
                "jcr:path") && !propertyName.equals("rep:facet");
    }

    private boolean isRegExp(NodeState nodeState) {
        PropertyState regExp = nodeState.getProperty("isRegexp");
        return regExp != null && regExp.getValue(Type.BOOLEAN);
    }

    private boolean hasValidPropertyNameNode(NodeState nodeState) {
        return nodeState.exists() && (nodeState.hasProperty(
                PROPERTY_NAME) || nodeState.hasProperty(
                PROPERTIES)) && !isRegExp(nodeState) && !hasVirtualProperty(
                nodeState);
    }

    private boolean hasVirtualProperty(NodeState nodeState) {
        return nodeState.hasProperty(
                PROPERTY_NAME) && VIRTUAL_PROPERTY_NAME.equals(getStringOrEmpty(
                nodeState.getProperty(PROPERTY_NAME))) || nodeState.hasProperty(
                PROPERTIES) && VIRTUAL_PROPERTY_NAME.equals(
                getStringOrEmpty(nodeState.getProperty(PROPERTIES)));
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

    private int getNumCols(Iterable<? extends PropertyState> properties) {
        return properties.iterator().hasNext() ?
               CountMinSketch.deserialize(properties.iterator()
                                                    .next()
                                                    .getValue(
                                                            Type.STRING)).length :
               0;
    }

    private int getNumRows(Iterable<? extends PropertyState> properties) {
        int count = 0;
        for (PropertyState ps : properties) {
            count++;
        }
        return count;
    }
}
