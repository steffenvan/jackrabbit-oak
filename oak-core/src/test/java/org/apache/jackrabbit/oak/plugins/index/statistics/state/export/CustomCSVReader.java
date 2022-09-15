package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.opencsv.CSVReader;

public class CustomCSVReader {

	static class PropertyCount {
		private String name;
		private int count;

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

	private String filename;
	private String delimiter;

	CustomCSVReader(String filename, String delimiter) {
		this.filename = filename;
		this.delimiter = delimiter;
	}

	List<String> read() throws FileNotFoundException, IOException {
		List<String> queries = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] row = line.split(delimiter);
				// a query starts with "". Skipping the first line of the CSV file if it's the
				// column name
				String query = row[1];

				query = query.replace("\"\"", "'");
				queries.add(query);
			}
		}

		return queries;
	}

	static void countIndexes(Map<String, Object> properties, Map<String, Integer> counts) {
//		Map<String, Integer> counts = new HashMap<>();

		for (Map.Entry<String, Object> e : properties.entrySet()) {
			counts.merge(e.getKey(), 1, Integer::sum);
		}
	}

	static Map<String, Integer> countProperties(List<String> props) {
		Map<String, Integer> counts = new HashMap<>();

		for (String s : props) {
			counts.merge(s, 1, Integer::sum);
		}

		return counts;
	}

	static List<String> getIndexes(Map<String, Object> indexRules) {
		List<String> indexes = new ArrayList<>();
		for (Map.Entry<String, Object> e : indexRules.entrySet()) {
			indexes.add(e.getKey());
		}
		return indexes;
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

	public static void mapToCSV(Map<String, Integer>, )

	public static void testSingleQuery(String query) throws Exception {
		List<Map<String, Object>> allIndexes = new ArrayList<>();
		Map<String, Integer> allIndexCounts = new HashMap<>();
		NodeState s = IndexConfigGeneratorHelper.getIndexConfig(query);
		Map<String, Object> index = NodeStateExporter.toMap(s);
		Map<String, Object> indexRules = (Map<String, Object>) index.get("indexRules");
		Map<String, Object> props = (Map<String, Object>) indexRules.get("properties");
		if (props != null) {
			CustomCSVReader.countIndexes(props, allIndexCounts);
		} else {
			CustomCSVReader.countIndexes(indexRules, allIndexCounts);
		}
		System.out.println(allIndexCounts);

	}

	public static void main(String[] args) throws Exception {
		Path path = Paths.get(
				"/Users/steffenvan/Documents/jackrabbit-oak/oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/statistics/state/export/queries2.csv");

		List<Map<String, Object>> allIndexes = new ArrayList<>();
		Map<String, Integer> allIndexCounts = new HashMap<>();
		List<String> failedQueries = new ArrayList<>();

		try {
			List<String> queries = readLineByLine(path);

			for (String q : queries) {
//				System.out.println(q);
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
		System.out.println(failedQueries.size());
//		System.out.println(allIndexCounts);

	}

}
