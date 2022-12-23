package org.apache.jackrabbit.oak.plugins.index.statistics;

import com.google.common.io.Files;
import org.apache.jackrabbit.oak.commons.json.JsonObject;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class IndexReader {
    private final List<PropertyStatistics> PropertyStatisticss;

    IndexReader(List<PropertyStatistics> results) {
        this.PropertyStatisticss = results;
    }

    public static void main(String[] args) throws IOException {
        String path = "/Users/steffenvan/aem/statistics0.json";
        String content = Files.asCharSource(new File(path),
                                            StandardCharsets.UTF_8).read();
        JsonObject obj = JsonObject.fromJson(content, true);
        Map<String, JsonObject> children = obj.getChildren()
                                              .get("properties")
                                              .getChildren();

        String test2 = "hello world";
        Map<String, String> myMap = new HashMap<>();
        myMap.put("foo", test2);

        String test = "[\"hello\", \"world\"]";
        String s = JsopTokenizer.decode(test);
        //        String[] t = (String[]) test;
        List<PropertyStatistics> results = new ArrayList<>();
        System.out.println(test.charAt(0));
        IndexReader ir = new IndexReader(results);
        ir.process(children);
    }

    //    ""0, 0, 0, 0""
    private TopKValues getTopKElements(Map<String, String> properties) {
        List<String> topValues = JsonArrayParser.readString(
                properties.get("mostFrequentValueNames"));
        List<Long> topCounts = JsonArrayParser.readNumbers(
                properties.get("mostFrequentValueCounts"));
        int k = topValues.size();

        return TopKValues.createFromIndex(topValues, topCounts, k);
    }

    void process(Map<String, JsonObject> currentNode) {
        if (currentNode == null) {
            return;
        }

        for (Map.Entry<String, JsonObject> e : currentNode.entrySet()) {
            String name = e.getKey();
            JsonObject child = e.getValue();
            PropertyStatistics er = readProperty(name, child);
            String something = "";
        }
    }

    private PropertyStatistics readProperty(String value, JsonObject node) {
        Map<String, String> properties = node.getProperties();
        long count = Long.parseLong(properties.get("count"));

        long valueLengthTotal = Long.parseLong(
                properties.get("valueLengthTotal"));
        long valueLengthMax = Long.parseLong(properties.get("valueLengthMax"));
        long valueLengthMin = Long.parseLong(properties.get("valueLengthMin"));
        String val = node.toString();
        System.out.println(val);
        System.out.println(properties.get("uniqueHLL"));
        String s = properties.get("uniqueHLL");
        TopKValues topKValues = getTopKElements(properties);
        byte[] hll = HyperLogLog.deserialize(s);
        System.out.println("hello");
        return null;
        //        PropertyStatistics ps = new PropertyStatistics(value,
        //        count, new HyperLogLog(hll.length, hll), getCms(properties)
        //        , getTopKElements(properties), valueLengthTotal,
        //        valueLengthMax, valueLengthMin);
        //
        //
        //        return new PropertyStatistics(value, ps.getCount(), ps
        //        .getCmsCount(Hash.hash64(value.hashCode())), ps.getHll()
        //                                                                                                           .estimate(), ps.getValueLengthTotal(), ps.getValueLengthMax(), ps.getValueLengthMin(), topKValues.get());
    }

    //    private CountMinSketch getCms(Map<String, String> properties) {
    //        int rows = Integer.parseInt(properties.get("valueSketchRows"));
    //        int cols = Integer.parseInt(properties.get("valueSketchCols"));
    //        long[][] data = new long[rows][cols];
    //
    //        for (int i = 0; i < rows; i++) {
    //            String cmsRow = properties.get("valueSketch" + i);
    //            data[i] = CountMinSketch.deserialize(cmsRow, mockLogger);
    //        }
    //        return new CountMinSketch(rows, cols, data);
    //    }
}
