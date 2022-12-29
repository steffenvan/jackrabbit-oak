package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Map;

public class NodeStateExporter {
    public static Map<String, Object> toMap(NodeState state) {
        return new JsonExporter().toMap(state);
    }
}
