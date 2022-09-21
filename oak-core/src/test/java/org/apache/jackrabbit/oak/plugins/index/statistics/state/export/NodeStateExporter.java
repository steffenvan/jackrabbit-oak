package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.util.Map;

import org.apache.jackrabbit.oak.spi.state.NodeState;

public class NodeStateExporter {

    public static String toJson(NodeState state) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    public static Map<String, Object> toMap(NodeState state) {
        return new JsonExporter().toMap(state);
    }

//    static String toCND(NodeState state){
//        return new CndExporter().toCNDFormat(state)
//    }
//
//    static String toXml(NodeState state){
//        return new XmlExporter().toXml(state)
//    }
}
