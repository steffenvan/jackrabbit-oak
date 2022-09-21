package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class JsonExporter {

    public String toJson(NodeState state) {
//        return JsonOutput.prettyPrint(JsonOutput.toJson(toMap(state)));
        throw new UnsupportedOperationException();
    }

    public Map<String, Object> toMap(NodeState state) {
        Map<String, Object> result = new HashMap<>();
        return copyNode(state, result);
    }

    private static Map<String, Object> copyNode(NodeState state, Map<String, Object> result) {
        copyProperties(state, result);
        for (ChildNodeEntry cne : state.getChildNodeEntries()) {
            Map<String, Object> nodeMap = new HashMap<>();
            result.put(cne.getName(), nodeMap);
            copyNode(cne.getNodeState(), nodeMap);
        }

        return result;
    }

    private static Map<String, Object> copyProperties(NodeState state, Map<String, Object> map) {
        for (PropertyState ps : state.getProperties()) {
            map.put(ps.getName(), ps.getValue(ps.getType()));
        }
        return map;
    }
}
