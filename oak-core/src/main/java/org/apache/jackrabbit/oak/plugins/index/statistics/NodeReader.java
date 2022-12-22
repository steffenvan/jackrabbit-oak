package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.Optional;

/**
 * Represents a set of utility functions that makes it easier to read the
 * oak:index node and the statistics index node.
 */
public class NodeReader {
    public static String getStringOrEmpty(PropertyState ps) {
        return ps == null ? "" : ps.getValue(Type.STRING);
    }

    public static NodeState getIndexNode(NodeStore store) {
        return store.getRoot()
                    .getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME);
    }

    public static Optional<NodeState> getStatisticsIndexDataNodeOrNull(
            NodeState indexNode) {
        if (!indexNode.exists()) {
            return Optional.empty();
        }
        NodeState statisticsIndexNode = indexNode.getChildNode(
                StatisticsEditorProvider.TYPE);
        if (!statisticsIndexNode.exists()) {
            return Optional.empty();
        }
        if (!StatisticsEditorProvider.TYPE.equals(
                statisticsIndexNode.getString("type"))) {
            return Optional.empty();
        }
        NodeState statisticsDataNode = statisticsIndexNode.getChildNode(
                StatisticsEditor.DATA_NODE_NAME);
        if (!statisticsDataNode.exists()) {
            return Optional.empty();
        }

        return Optional.of(statisticsDataNode);
    }
}
