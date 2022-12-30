package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getLong;
import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getString;

/**
 * Represents a set of utility functions that makes it easier to read the
 * oak:index node and the statistics index node.
 */
public class StatisticsIndexHelper {
    public static String getStringOrEmpty(NodeState nodeState, String name) {
        return Optional.ofNullable(getString(nodeState, name)).orElse("");
    }

    /**
     * Gets property stored at
     * oak:index/statistics/index/properties/{@code propertyName}. This is
     * called from the jmx bean.
     *
     * @param propertyName the name of the desired property
     * @param indexRoot    the oak:index node
     * @return the property stored under
     * oak:index/statistics/index/properties/{@code propertyName} if it exists.
     * Otherwise, a missing NodeState.
     */
    public static NodeState getPropertyNodeAtStatisticsIndex(
            String propertyName, NodeState indexRoot) {

        return getNodeFromIndexRoot(indexRoot).getChildNode(propertyName);
    }

    /**
     * Gets the estimated count of the property
     *
     * @param propertyName the propertyName that we want to get the estimated
     *                     count of
     * @param oakIndexNode the node oak:index/
     * @return the estimated CMS count of the property if the property exists.
     * Else -1.
     */
    public static long getCount(String propertyName, NodeState oakIndexNode) {
        NodeState statNode = oakIndexNode.getChildNode(
                StatisticsEditorProvider.TYPE).getChildNode("index");
        if (!statNode.exists()) {
            return -1;
        }

        CountMinSketch nameSketch = CountMinSketch.readCMS(statNode,
                                                           StatisticsEditor.PROPERTY_CMS_NAME,
                                                           StatisticsEditor.PROPERTY_CMS_ROWS_NAME,
                                                           StatisticsEditor.PROPERTY_CMS_COLS_NAME,
                                                           ContentStatistics.CS_LOG);

        return nameSketch.estimateCount(Hash.hash64(propertyName.hashCode()));
    }

    /**
     * Gets the top k values of a specific property
     *
     * @param propertyName the property for which to find the top k values
     * @param oakIndexNode the oak:index node
     * @return a list of the top K values of the properties if the property
     * exists. Else an empty list.
     */
    public static List<TopKValues.ValueCountPair> getTopValues(
            String propertyName, NodeState oakIndexNode) {
        NodeState propNode = getPropertyNodeAtStatisticsIndex(propertyName,
                                                              oakIndexNode);
        if (!propNode.exists()) {
            return Collections.emptyList();
        }

        PropertyState topKCounts = propNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_COUNT);
        PropertyState topKValues = propNode.getProperty(
                StatisticsEditor.PROPERTY_TOP_K_VALUES);

        int topK = Math.toIntExact(
                getLong(propNode, StatisticsEditor.PROPERTY_TOP_K));

        TopKValues topKElements = TopKValues.fromPropertyStates(topKValues,
                                                                topKCounts,
                                                                topK);

        return topKElements.get();
    }

    /**
     * Gets the property node: oak:index/statistics/index/properties.
     *
     * @param indexNode oak:index node
     * @return returns the node that is stored at
     * oak:index/statistics/index/properties if it exists. Otherwise, an empty
     * NodeState
     */
    public static NodeState getNodeFromIndexRoot(NodeState indexNode) {
        if (!indexNode.exists()) {
            return EmptyNodeState.MISSING_NODE;
        }
        NodeState statIndexNode = indexNode.getChildNode(
                StatisticsEditorProvider.TYPE);
        if (!statIndexNode.exists()) {
            return EmptyNodeState.MISSING_NODE;
        }

        if (!StatisticsEditorProvider.TYPE.equals(
                statIndexNode.getString("type"))) {
            return EmptyNodeState.MISSING_NODE;
        }

        NodeState statisticsDataNode = statIndexNode.getChildNode(
                StatisticsEditor.DATA_NODE_NAME);
        if (!statisticsDataNode.exists()) {
            return EmptyNodeState.MISSING_NODE;
        }

        statisticsDataNode = statisticsDataNode.getChildNode(
                StatisticsEditor.PROPERTIES);
        if (!statisticsDataNode.exists()) {
            return EmptyNodeState.MISSING_NODE;
        }

        return statisticsDataNode;
    }
}
