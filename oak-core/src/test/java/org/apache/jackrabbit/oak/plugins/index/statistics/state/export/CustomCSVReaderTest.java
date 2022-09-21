package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.junit.Assert.assertNotNull;

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
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.QueryEngine;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.statistics.StatisticsEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.ContentStatistics;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.EstimationResult;
import org.apache.jackrabbit.oak.plugins.index.statistics.state.export.CustomCSVReader.PropertyCount;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Before;
import org.junit.Test;

import com.opencsv.CSVReader;

public class CustomCSVReaderTest {
    public final static String INDEX_RULES = "indexRules";

    private NodeStore store;
    private ContentStatistics cs;
    Whiteboard wb;
    NodeStore nodeStore;
    Root root;
    QueryEngine qe;
    ContentSession session;

    @Before
    public void before() throws Exception {
        session = createRepository().login(null, null);
        root = session.getLatestRoot();
        qe = root.getQueryEngine();
        this.cs = new ContentStatistics(store);
    }

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

    static void listToCSV(List<EstimationResult> info, String filename) {
        String eol = System.getProperty("line.separator");

        try (Writer writer = new FileWriter(filename, true)) {
            for (EstimationResult er : info) {
                writer.append(er.getName()).append(',').append(Long.toString(er.getCount()))
                        .append(Long.toString(er.getHllCount())).append(eol);
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.err);
        }
    }

    @Test
    public void generatePropertiesCSV() {
        List<EstimationResult> estimationResults = cs.getAllPropertiesEstimation();
        String filename = "/Users/steffenvan/Documents/propertiesEstimation.csv";
        listToCSV(estimationResults, filename);
    }

    @Test
    public void test() throws Exception {
        Path path = Paths.get(
                "/Users/steffenvan/Documents/jackrabbit-oak/oak-core/src/test/java/org/apache/jackrabbit/oak/plugins/index/statistics/state/export/queries2.csv");

        List<Map<String, Object>> allIndexes = new ArrayList<>();
        Map<String, Integer> allIndexCounts = new HashMap<>();
        List<String> failedQueries = new ArrayList<>();

        List<String> queries = readLineByLine(path);

        for (String q : queries) {
            NodeState nodeState = IndexConfigGeneratorHelper.getIndexConfig(q);
            if (nodeState.hasChildNode(INDEX_RULES)) {
                Set<String> properties = cs.getIndexedProperties(nodeState);
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
        System.out.println(failedQueries.size());
        String filename = "/Users/steffenvan/Documents/output.csv";

        mapToCSV(allIndexCounts, filename);
//      System.out.println(allIndexCounts);
    }

    protected ContentRepository createRepository() throws CommitFailedException {
        nodeStore = new MemoryNodeStore();
        Oak oak = new Oak(nodeStore).with(new InitialContent()).with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider()).with(new StatisticsEditorProvider())
//                .with(new NodeCounterEditorProvider())
                // Effectively disable async indexing auto run
                // such that we can control run timing as per test requirement
                .withAsyncIndexing("async", TimeUnit.DAYS.toSeconds(1));

//        mergeIndex("statistics");
        NodeBuilder builder = nodeStore.getRoot().builder();
        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);

        index.child("statistics").setProperty(JcrConstants.JCR_PRIMARYTYPE, INDEX_DEFINITIONS_NODE_TYPE, NAME)
                .setProperty(TYPE_PROPERTY_NAME, StatisticsEditorProvider.TYPE)
                .setProperty(IndexConstants.ASYNC_PROPERTY_NAME, IndexConstants.ASYNC_PROPERTY_NAME)
                .setProperty("info", "STATISTICS");

        nodeStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        wb = oak.getWhiteboard();

        return oak.createContentRepository();
    }

    private void runAsyncIndex() {
        Runnable async = WhiteboardUtils.getService(wb, Runnable.class, new Predicate<Runnable>() {
            @Override
            public boolean test(@Nullable Runnable input) {
                return input instanceof AsyncIndexUpdate;
            }
        });
        assertNotNull(async);
        async.run();
        root.refresh();
    }
}
