package org.apache.jackrabbit.oak.plugins.index.statistics.generator;

import java.util.List;
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

//	@Test
//	public void multipleQueries() throws Exception {
//		dumpIndex(
//				"SELECT * FROM [nt:base] AS a WHERE a.[jcr:content/metadata/status] = 'published' ORDER BY a.[jcr:content/metadata/jcr:lastModified] DESC select * from [nt:base] where foo = 1");
//	}

	private void dumpIndex(String queryText) throws Exception {
		NodeState state = IndexConfigGeneratorHelper.getIndexConfig(queryText);
		Map<String, Object> json = NodeStateExporter.toMap(state);
		List<String> indexes = getIndexes(json);
//		PrettyPrintingMap<String, Object> map = new PrettyPrintingMap<String, Object>(json);
//		System.out.println(map);
//		json.forEach((k, v) -> System.out.println("Key: " + k + ": Value: " + v));

//		JSONObject object = new JSONObject();
////		JSONParser parser = new JSONParser();
//		object.put("test", json);
//		String jsonString = object.toJSONString();
////		Object obj = parser.parse(json);
////		Map<String, Object> map = (Map<String, Object>) obj;
//		System.out.println(jsonString);
//		PrintWriter writer = new PrintWriter(map);
//		json.prettyPrint(map);
	}

	@Test
	public void xpath() throws Exception {
		dumpIndex("/jcr:root/content/dam/element(*, dam:Asset)[@valid]");
	}

	private List<String> getIndexes(Map<String, Object> map) {
		Map<String, Object> indexRules = (Map<String, Object>) map.get("indexRules");
		System.out.println(indexRules);
		return List.of("");
//		for (Map.Entry<String, Object> e : indexRules.entrySet()) {
//			System
//		}
	}

//	@Test
//    public void multi2() throws Exception{
//        dumpIndex("#Paste your queries here SELECT * FROM [dam:Asset] AS a WHERE a.[jcr:content/metadata/status] = 'published\' ORDER BY a.[jcr:content/metadata/jcr:lastModified] DESC SELECT
//  *
//FROM [dam:Asset]
//WHERE
//  CONTAINS([mimetype], 'text/plain')
//
//# You can also include xpath queries
///jcr:root/content/dam/element(*, dam:Asset)[@valid]")
//    }

}
