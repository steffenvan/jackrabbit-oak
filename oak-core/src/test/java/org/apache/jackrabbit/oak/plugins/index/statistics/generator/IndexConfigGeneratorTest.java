package org.apache.jackrabbit.oak.plugins.index.statistics.generator;

import java.text.ParseException;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.index.statistics.state.export.NodeStateExporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexConfigGeneratorTest {

	@After
	public void dumpIndexConfig() {
		Map<String, Object> res = NodeStateExporter.toMap(generator.getIndexConfig());
		System.out.println(res);
	}

	IndexConfigGenerator generator;

	@Before
	public void before() throws Exception {
		generator = new IndexConfigGenerator();
	}

	@Test
	public void generateIndexConfig() throws ParseException {
		generator.process("select * from [nt:base] where foo = 'bar'");
	}

	@Test
	public void generateIndexConfig2() throws ParseException {
//		        generator.process("SELECT  *  FROM [dam:Asset] AS a  WHERE   a.[jcr:content/metadata/status] = 'published'  ORDER BY  a.[jcr:content/metadata/jcr:lastModified] DESC")
		generator.process("SELECT  *  FROM [dam:Asset] AS a  WHERE   a.[jcr:content/metadata/status] = 'published'  "
				+ "ORDER BY  a.[jcr:content/metadata/jcr:lastModified] DESC");
	}

	@Test
	public void fulltext() throws Exception {
		generator.process("SELECT *FROM [app:Asset] WHERE CONTAINS([jcr:content/metadata/comment], 'december')");
	}

	@Test
	public void fulltext2() throws Exception {
		generator.process("SELECT* FROM [app:Asset] WHERE CONTAINS([jcr:content/metadata/*], 'december')");
	}

	@Test
	public void fulltext3() throws Exception {
		generator.process("SELECT * FROM [app:Asset] WHERE CONTAINS([month], 'december')");
	}

}
