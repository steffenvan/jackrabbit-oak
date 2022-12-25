package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.Optional;

import static org.apache.jackrabbit.oak.spi.state.AbstractNodeState.getString;

/**
 * Represents a set of utility functions that makes it easier to read the
 * oak:index node and the statistics index node.
 */
public class StatisticsNodeHelper {
    public static String getStringOrEmpty(NodeState nodeState, String name) {
        return Optional.ofNullable(getString(nodeState, name)).orElse("");
    }

    /**
     * Gets the root node of all indexes: oak:index.
     *
     * @param store the relevant NodeStore
     * @return the oak:index node from the provided NodeStore
     */
    public static NodeState getIndexRoot(NodeStore store) {
        return store.getRoot()
                    .getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
    }

    /**
     * Gets property stored at
     * oak:index/statistics/index/properties/{@code propertyName}. This is
     * called from the jmx bean.
     *
     * @param propertyName the name of the desired property
     * @param store        the store associated with the jmx bean
     * @return the property stored under
     * oak:index/statistics/index/properties/{@code propertyName} if it exists.
     * Otherwise, a missing NodeState.
     */
    public static NodeState getPropertyNodeFromStatisticsIndex(
            String propertyName, NodeStore store) {

        return getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
                getIndexRoot(store)).getChildNode(propertyName);
    }

    /**
     * Gets an indexed property. It assumes that the property is stored at
     * oak:index/{@code indexName}
     *
     * @param indexName the indexed property name
     * @param store     the relevant nodeStore
     * @return the indexed property node if it is indexed. Else a missing node.
     */
    public static NodeState getIndexedNodeFromName(String indexName,
                                                   NodeStore store) {
        return getIndexRoot(store).getChildNode(indexName);
    }

    /**
     * Gets the property node: oak:index/statistics/index/properties.
     *
     * @param indexNode oak:index node
     * @return returns the node that is stored at
     * oak:index/statistics/index/properties if it exists. Otherwise, an empty
     * NodeState
     */
    public static NodeState getStatisticsIndexDataNodeOrMissingFromOakIndexPath(
            NodeState indexNode) {
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
