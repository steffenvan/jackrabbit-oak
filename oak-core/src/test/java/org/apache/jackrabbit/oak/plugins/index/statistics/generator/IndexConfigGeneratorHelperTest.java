package org.apache.jackrabbit.oak.plugins.index.statistics.generator;

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.statistics.state.export.NodeStateExporter;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class IndexConfigGeneratorHelperTest {

	@Test
	public void simpleCase() throws Exception {
		dumpIndex("select * from [nt:base] where foo = 'bar'");
		String q = "SELECT\n" + "  *\n" + "FROM [dam:Asset] AS a\n" + "WHERE\n"
				+ "  a.[jcr:content/metadata/status] = 'published'\n" + "ORDER BY\n"
				+ "  a.[jcr:content/metadata/jcr:lastModified] DESC";

		dumpIndex(q);
	}

	private void dumpIndex(String queryText) throws Exception {
		NodeState state = IndexConfigGeneratorHelper.getIndexConfig(queryText);
		Map<String, Object> json = NodeStateExporter.toMap(state);
		Map<String, Object> indexRules = (Map<String, Object>) json.get("indexRules");
		System.out.println(indexRules);
	}

	@Test
	public void xpath() throws Exception {
		dumpIndex("/jcr:root/content/dam/element(*, dam:Asset)[@valid]");
	}
}
