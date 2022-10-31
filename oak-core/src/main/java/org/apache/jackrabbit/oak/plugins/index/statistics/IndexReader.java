package org.apache.jackrabbit.oak.plugins.index.statistics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Iterator;

import com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.EstimationResult;

class IndexReader {
    private final List<EstimationResult> estimationResults;

    IndexReader(List<EstimationResult> results) {
        this.estimationResults = results;
    }

    private TopKElements getTopKElements(JsonNode node) {
        JsonNode topValueNamesNode = node.path("mostFrequentValueNames");
        JsonNode topValueCountsNode = node.path("mostFrequentValueCounts");
        int k = topValueNamesNode.size();
        List<String> values = new ArrayList<>();
        List<Long> counts = new ArrayList<>();

        topValueCountsNode = node.path("count");
        if (!topValueNamesNode.isArray()) {
            throw new IllegalArgumentException("Invalid type: " + topValueNamesNode.getNodeType() +  " for top value names. It should have type: " + JsonNodeType.ARRAY);
        }

        if (!topValueCountsNode.isArray()) {
            throw new IllegalArgumentException("Invalid type: " + topValueCountsNode.getNodeType() +  " for top value counts. It should have type: " + JsonNodeType.ARRAY);
        }
        
        ArrayNode valuesArrayNode = (ArrayNode) topValueNamesNode;
        ArrayNode countsArrayNode = (ArrayNode) topValueCountsNode;
        valuesArrayNode.elements().forEachRemaining(entry -> values.add(entry.toString()));
        countsArrayNode.elements().forEachRemaining(entry -> counts.add(entry.asLong()));

        PriorityQueue<TopKElements.ValueCountPair> pqElements = TopKElements.deserialize(values, counts, k);
        return new TopKElements(pqElements, k, new HashSet<>());
    }

    private CountMinSketch getCms(JsonNode node) {
        int rows = node.path("valueSketchRows").asInt();
        int cols = node.path("valueSketchCols").asInt();
        long[][] data = new long[rows][cols];

        for (int i = 0; i < rows; i++){
            String cmsRow = node.path("valueSketch" + i).asText();
            data[i] = CountMinSketch.deserialize(cmsRow);
        }
        return new CountMinSketch(rows, cols, data);
    }
    private EstimationResult readProperty(String value, JsonNode node) {
        byte[] hll = HyperLogLog.deserialize(node.path("uniqueHLL").asText());
        long count = node.path("count").asLong();
        long valueTotalLength = node.path("valueTotalLength").asLong();
        long valueMaxLength = node.path("valueMaxLength").asLong();
        long valueMinLength = node.path("valueMinLength").asLong();
        PropertyStatistics ps = new PropertyStatistics(value, count, new HyperLogLog(hll.length, hll), getCms(node), getTopKElements(node), valueTotalLength, valueMaxLength, valueMinLength);

        return new EstimationResult(value, ps.getCount(), ps.getHll().estimate(), ps.getValueLengthTotal(), ps.getValueLengthMax(), ps.getValueLengthMin());
    }

    private void process(String prefix, JsonNode currentNode) {
        if (!prefix.isEmpty() && prefix.split("-").length == 1) {
            currentNode.fields().forEachRemaining(entry -> estimationResults.add(readProperty(entry.getKey(), entry.getValue())));
        }
        else if (currentNode.isObject()) {
            currentNode.fields().forEachRemaining(entry -> process(!prefix.isEmpty() ? prefix + "-" + entry.getKey() : entry.getKey(), entry.getValue()));
        }
        else if (currentNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) currentNode;
            Iterator<JsonNode> node = arrayNode.elements();
            int index = 1;
            while (node.hasNext()) {
                if (!prefix.isEmpty()) {
                    process(prefix + "-" + index, node.next());
                } else {
                    process(String.valueOf(index), node.next());
                }
                index += 1;
            }
        }
    }
    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        File newState = new File("/Users/steffenvan/aem/stat.json");
        JsonNode node = objectMapper.readTree(newState);
        List<EstimationResult> results = new ArrayList<>();
        IndexReader ir = new IndexReader(results);
        ir.process("", node);

        PrintStream o = new PrintStream("/Users/steffenvan/aem/properties_large.json");
        System.setOut(o);
        System.out.println(results);

        System.setOut(System.out);
    }
}
