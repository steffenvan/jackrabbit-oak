package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import com.opencsv.CSVReader;
import org.apache.jackrabbit.oak.plugins.index.statistics.IndexUtil;
import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.state.export.CustomCSVReader.PropertyCount;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;

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
import java.util.Set;

public class CustomCSVReaderTest {
    public final static String INDEX_RULES = "indexRules";

    private NodeStore store;
    private ContentStatistics cs;

    public static List<String> readLineByLine(Path filePath) throws Exception {
        List<String> list = new ArrayList<>();
        try (Reader reader = Files.newBufferedReader(filePath)) {
            try (CSVReader csvReader = new CSVReader(reader)) {
                String[] line;
                while ((line = csvReader.readNext()) != null) {
                    line[0] = line[0].replaceAll("\\$[a-zA-Z]+", "'x'");
                    line[0] = line[0].replace("CAST('x' AS DATE)",
                                              "CAST('2022-07-01T20:00:00" +
                                                      ".000'" + " AS DATE)");
                    line[0] = line[0].replace("cast('x' as date)",
                                              "cast('2022-07-01T20:00:00" +
                                                      ".000'" + " as date)");
                    line[0] = line[0].replace("CAST('x' AS BOOLEAN)",
                                              "CAST('TRUE' AS BOOLEAN)");
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
                writer.append(entry.getKey())
                      .append(',')
                      .append(entry.getValue().toString())
                      .append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    @Before
    public void before() throws Exception {
        this.store = new MemoryNodeStore();
        this.cs = new ContentStatistics(store);
    }

    public void test() throws Exception {
        // hard coded path to the CSV file that contains the queries from Splunk
        Path path = Paths.get(
                "/Users/steffenvan/aem/jackrabbit-oak/oak-core/src/test/java" + "/org/apache/jackrabbit/oak/plugins/index/statistics" + "/state/export/queries.csv");

        Map<String, Integer> allIndexCounts = new HashMap<>();
        List<String> failedQueries = new ArrayList<>();

        List<String> queries = readLineByLine(path);

        for (String q : queries) {
            NodeState nodeState = IndexConfigGeneratorHelper.getIndexConfig(q);
            if (nodeState.hasChildNode(INDEX_RULES)) {
                Set<String> properties = IndexUtil.getNames(nodeState);
                for (String s : properties) {
                    allIndexCounts.merge(s, 1, Integer::sum);
                }
            }
        }

        List<PropertyCount> counts = new ArrayList<>();
        for (Map.Entry<String, Integer> e : allIndexCounts.entrySet()) {
            counts.add(new PropertyCount(e.getKey(), e.getValue()));
        }
        counts.sort(Comparator.comparing(PropertyCount::getCount).reversed());
        System.out.println(counts);
        System.out.println(allIndexCounts);
        //        System.out.println(failedQueries.size());
        //        String filename = "/Users/steffenvan/Documents/output.csv";

        //        mapToCSV(allIndexCounts, filename);
        //      System.out.println(allIndexCounts);
    }
}
