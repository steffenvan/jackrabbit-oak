package org.apache.jackrabbit.oak.plugins.index.statistics.jmx;

import org.apache.felix.inventory.Format;
import org.apache.felix.inventory.InventoryPrinter;
import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.json.Base64BlobSerializer;
import org.apache.jackrabbit.oak.json.JsonSerializer;
import org.apache.jackrabbit.oak.plugins.index.statistics.NodeReader;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;

import java.io.PrintWriter;
import java.util.Optional;

@Component(service = InventoryPrinter.class,
           property = {"felix.inventory" + ".printer.name=oak-statistics",
                   "felix.inventory.printer" + ".title" + "=Statistics Index"
                   , "felix" + ".inventory.printer.format=JSON"})
public class StatisticsDefinitionPrinter implements InventoryPrinter {

    @Reference
    private NodeStore nodeStore;
    private String filter = "{\"properties\":[\"*\", \"-:childOrder\"]," +
            "\"nodes\":[\"*\", \"-:*\"]}";

    public StatisticsDefinitionPrinter() {

    }

    public StatisticsDefinitionPrinter(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
    }

    @Override
    public void print(PrintWriter printWriter, Format format, boolean isZip) {
        if (format == Format.JSON) {
            JsopBuilder json = new JsopBuilder();

            // oak:index/statistics/index
            Optional<NodeState> statisticsNode =
                    NodeReader.getStatisticsIndexDataNodeOrNull(
                    NodeReader.getIndexNode(nodeStore));

            statisticsNode.ifPresent(node -> {
                if (node.hasChildNode(StatisticsEditor.PROPERTIES)) {
                    createSerializer(json).serialize(node);
                    printWriter.print(JsopBuilder.prettyPrint(json.toString()));
                }
            });
        }
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    private JsonSerializer createSerializer(JsopBuilder json) {
        return new JsonSerializer(json, filter, new Base64BlobSerializer());
    }
}
