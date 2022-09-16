package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.plugins.index.statistics.state.export.CustomCSVReader.PropertyCount;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.opencsv.CSVReader;

public class CustomCSVReaderTest {

    public static List<String> readLineByLine(Path filePath) throws Exception {
        List<String> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReader(reader)) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    line[0] = line[0].replaceAll("\\$[a-zA-Z]+", "'x'");
                    line[0] = line[0].replace("CAST('x' AS DATE)", "CAST('2022-07-01T20:00:00.000' AS DATE)");
                    line[0] = line[0].replace("cast('x' as date)", "cast('2022-07-01T20:00:00.000' as date)");
                    line[0] = line[0].replace("CAST('x' AS BOOLEAN)", "CAST('TRUE' AS BOOLEAN)");
                    line[0] = line[0].replace(", 'x'", ", '/x'");
                    line[0] = line[0].replace(",'x'", ", '/x'");
                    list.add(line[0]);
                }
            }
        }
        return list;
    }

    public static void mapToCSV(Map<String, Integer> infos, String filename) {
        String eol = System.getProperty("line.separator");

        try (Writer writer = new FileWriter(filename, true)) {
            for (Map.Entry<String, Integer> entry : infos.entrySet()) {
                writer.append(entry.getKey()).append(',').append(entry.getValue().toString()).append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    @Test
    public void test() {
        Path path = Paths.get(
                "/Users/steffenvan/Documents/jackrabbit-oak/oak-core/src/test/java/org/apache/jackrabbit/oak/plugins/index/statistics/state/export/queries2.csv");

        List<Map<String, Object>> allIndexes = new ArrayList<>();
        Map<String, Integer> allIndexCounts = new HashMap<>();
        List<String> failedQueries = new ArrayList<>();

        try {
            List<String> queries = readLineByLine(path);

            for (String q : queries) {
//              System.out.println(q);
                try {
                    NodeState s = IndexConfigGeneratorHelper.getIndexConfig(q);
                    Map<String, Object> index = NodeStateExporter.toMap(s);
                    List<String> indexes = CustomCSVReader.getIndexes(index);
                    Map<String, Object> indexRules = (Map<String, Object>) index.get("indexRules");
                    Map<String, Object> props = (Map<String, Object>) indexRules.get("properties");
                    if (props != null) {
                        CustomCSVReader.countIndexes(props, allIndexCounts);
                    } else {
                        CustomCSVReader.countIndexes(indexRules, allIndexCounts);
                    }
                } catch (Exception e) {
                    failedQueries.add(q);
                    System.out.println(q);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("FAILED QUERIES: " + failedQueries);
        for (String s : failedQueries) {
            System.out.println(s);
        }

        List<PropertyCount> counts = new ArrayList<>();
        for (Map.Entry<String, Integer> e : allIndexCounts.entrySet()) {
            counts.add(new PropertyCount(e.getKey(), e.getValue()));
        }
        counts.sort(Comparator.comparing(PropertyCount::getCount).reversed());
        System.out.println(counts);
        System.out.println(allIndexCounts);
        System.out.println(failedQueries.size());
        String filename = "/Users/steffenvan/Documents/output.csv";
        mapToCSV(allIndexCounts, filename);
//      System.out.println(allIndexCounts);
    }
}
