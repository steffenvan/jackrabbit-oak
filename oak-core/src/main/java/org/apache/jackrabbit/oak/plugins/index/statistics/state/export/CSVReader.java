package org.apache.jackrabbit.oak.plugins.index.statistics.state.export;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.statistics.generator.IndexConfigGeneratorHelper;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class CSVReader {

	private String filename;
	private String delimiter;

	CSVReader(String filename, String delimiter) {
		this.filename = filename;
		this.delimiter = delimiter;
	}

	List<List<String>> read() throws FileNotFoundException, IOException {
		List<List<String>> records = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
			String line;
			while ((line = br.readLine()) != null) {
				String[] row = line.split(delimiter);
				records.add(Arrays.asList(row));
			}
		}

		return records;
	}

	public static void main(String[] args) throws Exception {
		String path = "/Users/steffenvan/Documents/jackrabbit-oak/oak-core/src/main/java/org/apache/jackrabbit/oak/plugins/index/statistics/state/export/queries.csv";
		CSVReader reader = new CSVReader(path, ",");
		List<List<String>> records = reader.read();
		System.out.println(records);
		for (List<String> queries : records.subList(1, records.size())) {
			NodeState s = IndexConfigGeneratorHelper.getIndexConfig(queries.get(1));
			Map<String, Object> res = NodeStateExporter.toMap(s);
			System.out.println(res);
		}
	}

}
