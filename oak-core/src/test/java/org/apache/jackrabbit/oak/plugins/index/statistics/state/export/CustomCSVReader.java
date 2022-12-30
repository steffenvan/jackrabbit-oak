package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import com.opencsv.CSVReader;
import org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil;
import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides functionality to read a csv file that contain queries and
 * output which and how many times a property is queried with those queries. We
 * use the IndexConfigGenerator to generate the index definitions. The queries
 * in the .csv file could come from customers or be custom.
 */
public class CustomCSVReader {
    public final static String INDEX_RULES = "indexRules";

    public static void main(String[] args) throws Exception {
        String oakPath = System.getProperty("user.dir");
        String csvPath = "oak-core/src/test/java" + "/org/apache/jackrabbit/oak/plugins/index/statistics" + "/state/export/queries.csv";
        Path path = Paths.get(oakPath, csvPath);

        Map<String, Integer> allIndexCounts = new HashMap<>();
        List<String> queries = readFile(path);

        for (String q : queries) {
            NodeState nodeState = IndexConfigGeneratorHelper.getIndexConfig(q);
            if (nodeState.hasChildNode(INDEX_RULES)) {
                Set<String> properties = IndexUtil.getNames(nodeState);
                for (String s : properties) {
                    allIndexCounts.merge(s, 1, Integer::sum);
                }
            }
        }

        List<PropertyCount> counts = getQueryCountsSorted(allIndexCounts);
        System.out.println(counts);
    }

    private static List<PropertyCount> getQueryCountsSorted(
            Map<String, Integer> allIndexCounts) {
        List<PropertyCount> counts = new ArrayList<>();
        for (Map.Entry<String, Integer> e : allIndexCounts.entrySet()) {
            counts.add(new PropertyCount(e.getKey(), e.getValue()));
        }
        counts.sort(Comparator.comparing(PropertyCount::getCount).reversed());

        return counts;
    }

    private static List<String> readFile(Path filePath) throws Exception {
        List<String> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReader(reader)) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    list.add(readLine(line));
                }
            }
        }
        return list;
    }

    private static String readLine(String[] line) {
        line[0] = line[0].replaceAll("\\$[a-zA-Z]+", "'x'");
        line[0] = line[0].replace("CAST('x' AS DATE)",
                                  "CAST('2022-07-01T20:00:00" + ".000'" + " AS DATE)");
        line[0] = line[0].replace("cast('x' as date)",
                                  "cast('2022-07-01T20:00:00" + ".000'" + " as date)");
        line[0] = line[0].replace("CAST('x' AS BOOLEAN)",
                                  "CAST('TRUE' AS BOOLEAN)");
        line[0] = line[0].replace(", 'x'", ", '/x'");
        line[0] = line[0].replace(",'x'", ", '/x'");
        return line[0];
    }

    static class PropertyCount {
        private final String name;
        private final int count;

        PropertyCount(String name, int count) {
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public int getCount() {
            return count;
        }

        @Override
        public String toString() {
            return name + "=" + count;
        }
    }

}
