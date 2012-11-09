package com.datastax.driver.core;

import java.util.*;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test we correctly process and print schema.
 */
public class SchemaTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final Map<String, String> cql3 = new HashMap<String, String>();
    private static final Map<String, String> compact = new HashMap<String, String>();

    protected Collection<String> getTableDefinitions() {

        String sparse = "CREATE TABLE sparse (\n"
                      + "    k text,\n"
                      + "    c1 int,\n"
                      + "    c2 float,\n"
                      + "    l list<text>,\n"
                      + "    v int,\n"
                      + "    PRIMARY KEY (k, c1, c2)\n"
                      + ")";

        String st = "CREATE TABLE static (\n"
                  + "    k text,\n"
                  + "    i int,\n"
                  + "    m map<text, timeuuid>,\n"
                  + "    v int,\n"
                  + "    PRIMARY KEY (k)\n"
                  + ")";

        String compactStatic = "CREATE TABLE compact_static (\n"
                             + "    k text,\n"
                             + "    i int,\n"
                             + "    t timeuuid,\n"
                             + "    v int,\n"
                             + "    PRIMARY KEY (k)\n"
                             + ") WITH COMPACT STORAGE";

        String compactDynamic = "CREATE TABLE compact_dynamic (\n"
                              + "    k text,\n"
                              + "    c int,\n"
                              + "    v timeuuid,\n"
                              + "    PRIMARY KEY (k, c)\n"
                              + ") WITH COMPACT STORAGE";

        String compactComposite = "CREATE TABLE compact_composite (\n"
                                + "    k text,\n"
                                + "    c1 int,\n"
                                + "    c2 float,\n"
                                + "    c3 double,\n"
                                + "    v timeuuid,\n"
                                + "    PRIMARY KEY (k, c1, c2, c3)\n"
                                + ") WITH COMPACT STORAGE";

        cql3.put("sparse", sparse);
        cql3.put("static", st);
        compact.put("compact_static", compactStatic);
        compact.put("compact_dynamic", compactDynamic);
        compact.put("compact_composite", compactComposite);

        List<String> allDefs = new ArrayList<String>();
        allDefs.addAll(cql3.values());
        allDefs.addAll(compact.values());
        return allDefs;
    }

    private static String stripOptions(String def, boolean keepFirst) {
        if (keepFirst)
            return def.split("\n   AND ")[0];
        else
            return def.split(" WITH ")[0];
    }

    // Note: this test is a bit fragile in the sense that it rely on the exact
    // string formatting of exportAsString, but it's a very simple/convenient
    // way to check we correctly handle schemas so it's probably not so bad.
    // In particular, exportAsString *does not* guarantee that you'll get
    // exactly the same string than the one used to create the table.
    @Test
    public void schemaExportTest() {

        KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE);

        for (Map.Entry<String, String> tableEntry : cql3.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(def, stripOptions(metadata.getTable(table).exportAsString(), false));
        }

        for (Map.Entry<String, String> tableEntry : compact.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(def, stripOptions(metadata.getTable(table).exportAsString(), true));
        }
    }
}
