package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.util.Set;
import java.util.TreeSet;

import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor.PROPERTIES;
import static org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsIndexHelper.getStringOrEmpty;

public class IndexUtil {
    private static final String INDEX_RULES = "indexRules";
    private static final String PROPERTY_NAME = "name";
    private static final String VIRTUAL_PROPERTY_NAME = ":nodeName";

    /**
     * Gets the properties that are stored at an index. To find these properties
     * we traverse the index node's children until we find nodes that are
     * "valid". A child node is considered valid, if it fulfills certain
     * criteria, like having a "name" property that does not start with
     * "function". For further details, see:
     * <p>
     * {@link #hasValidPropertyNameNode(NodeState)} and
     * {@link #isValidPropertyName(String)}
     *
     * @param nodeState - the node state of the indexed property. E.g.
     *                  oak:index/counter, oak:index/socialLucene etc.
     * @return the properties that are indexed under the specified index node.
     */
    public static Set<String> getNames(NodeState nodeState) {
        Set<String> names = new TreeSet<>();
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
                            propertyName = parse(
                                    getStringOrEmpty(childNode, PROPERTY_NAME));
                        } else {
                            propertyName = parse(
                                    getStringOrEmpty(childNode, PROPERTIES));
                        }
                        if (isValidPropertyName(propertyName)) {
                            names.add(propertyName);
                        }
                    }
                }
            }
        }
        return names;
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
     * Gets an indexed property. It assumes that the property is stored at
     * oak:index/{@code indexName}
     *
     * @param indexName the indexed property name
     * @param store     the relevant nodeStore
     * @return the indexed property node if it is indexed. Else a missing node.
     */
    public static NodeState getIndexedNodeFromName(String indexName,
                                                   NodeStore store) {
        return IndexUtil.getIndexRoot(store).getChildNode(indexName);
    }

    static String parse(String propertyName) {
        if (propertyName.contains("/")) {
            String[] parts = propertyName.split("/");
            return parts[parts.length - 1];
        }
        return propertyName;
    }

    static boolean isValidPropertyName(String propertyName) {
        return !propertyName.startsWith("function") && !propertyName.equals(
                "jcr:path") && !propertyName.equals("rep:facet");
    }

    static boolean isRegExp(NodeState nodeState) {
        PropertyState regExp = nodeState.getProperty("isRegexp");
        return regExp != null && regExp.getValue(Type.BOOLEAN);
    }

    static boolean hasValidPropertyNameNode(NodeState nodeState) {
        return nodeState.exists() && (nodeState.hasProperty(
                PROPERTY_NAME) || nodeState.hasProperty(
                PROPERTIES)) && !isRegExp(nodeState) && !hasVirtualProperty(
                nodeState);
    }

    static boolean hasVirtualProperty(NodeState nodeState) {
        return nodeState.hasProperty(
                PROPERTY_NAME) && VIRTUAL_PROPERTY_NAME.equals(
                getStringOrEmpty(nodeState,
                                 PROPERTY_NAME)) || nodeState.hasProperty(
                PROPERTIES) && VIRTUAL_PROPERTY_NAME.equals(
                getStringOrEmpty(nodeState, PROPERTIES));
    }

    /*
     starting from the "indexRule" node, we perform a simple DFS to find the
     corresponding properties node of the index node.
     */
    private static NodeState getPropertiesNode(NodeState nodeState) {
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
}
