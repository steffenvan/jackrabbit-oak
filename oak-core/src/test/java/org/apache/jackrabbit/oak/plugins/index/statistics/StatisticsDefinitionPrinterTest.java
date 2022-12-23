package org.apache.jackrabbit.oak.plugins.index.statistics;

import org.apache.felix.inventory.Format;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.statistics.jmx.StatisticsDefinitionPrinter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class StatisticsDefinitionPrinterTest {
    NodeStore store;
    TestUtility utility;
    StatisticsDefinitionPrinter printer;

    @Before
    public void before() throws Exception {
        store = new MemoryNodeStore();
        printer = new StatisticsDefinitionPrinter(store);
        utility = new TestUtility(store, "statistics");
    }
    
    @Test
    public void testPrinter() throws CommitFailedException, ParseException {
        utility.addNodes();
        String json = getStatsIndexInfo();

        //If there is any error in rendered json exception would fail the test
        JSONObject o = (JSONObject) JSONValue.parseWithException(json);
        assertNull(o.get("nothing"));

        String row0 = o.get(StatisticsEditor.PROPERTY_CMS_NAME + "0")
                       .toString();

        assertNotNull(row0);
        assertNotNull(o.get(StatisticsEditor.PROPERTY_CMS_ROWS_NAME));
    }

    private String getStatsIndexInfo() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printer.print(pw, Format.JSON, false);

        pw.flush();

        return sw.toString();
    }

}
