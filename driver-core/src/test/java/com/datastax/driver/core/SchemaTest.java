/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Test we correctly process and print schema.
 */
public class SchemaTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final Map<String, String> cql3 = new HashMap<String, String>();
    private static final Map<String, String> compact = new HashMap<String, String>();

    private static String withOptions;

    protected Collection<String> getTableDefinitions() {

        String sparse = "CREATE TABLE sparse (\n"
                      + "    k text,\n"
                      + "    c1 int,\n"
                      + "    c2 float,\n"
                      + "    l list<text>,\n"
                      + "    v int,\n"
                      + "    PRIMARY KEY (k, c1, c2)\n"
                      + ");";

        String st = "CREATE TABLE static (\n"
                  + "    k text,\n"
                  + "    i int,\n"
                  + "    m map<text, timeuuid>,\n"
                  + "    v int,\n"
                  + "    PRIMARY KEY (k)\n"
                  + ");";

        String compactStatic = "CREATE TABLE compact_static (\n"
                             + "    k text,\n"
                             + "    i int,\n"
                             + "    t timeuuid,\n"
                             + "    v int,\n"
                             + "    PRIMARY KEY (k)\n"
                             + ") WITH COMPACT STORAGE;";

        String compactDynamic = "CREATE TABLE compact_dynamic (\n"
                              + "    k text,\n"
                              + "    c int,\n"
                              + "    v timeuuid,\n"
                              + "    PRIMARY KEY (k, c)\n"
                              + ") WITH COMPACT STORAGE;";

        String compactComposite = "CREATE TABLE compact_composite (\n"
                                + "    k text,\n"
                                + "    c1 int,\n"
                                + "    c2 float,\n"
                                + "    c3 double,\n"
                                + "    v timeuuid,\n"
                                + "    PRIMARY KEY (k, c1, c2, c3)\n"
                                + ") WITH COMPACT STORAGE;";

        cql3.put("sparse", sparse);
        cql3.put("static", st);
        compact.put("compact_static", compactStatic);
        compact.put("compact_dynamic", compactDynamic);
        compact.put("compact_composite", compactComposite);

        withOptions = "CREATE TABLE with_options (\n"
                    + "    k text,\n"
                    + "    i int,\n"
                    + "    PRIMARY KEY (k)\n"
                    + ") WITH read_repair_chance = 0.5\n"
                    + "   AND dclocal_read_repair_chance = 0.6\n"
                    + "   AND replicate_on_write = true\n"
                    + "   AND gc_grace_seconds = 42\n"
                    + "   AND bloom_filter_fp_chance = 0.01\n"
                    + "   AND caching = ALL\n"
                    + "   AND comment = 'My awesome table'\n"
                    + "   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 15 }\n"
                    + "   AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor', 'chunk_length_kb' : 128 };";

        List<String> allDefs = new ArrayList<String>();
        allDefs.addAll(cql3.values());
        allDefs.addAll(compact.values());
        allDefs.add(withOptions);
        return allDefs;
    }

    private static String stripOptions(String def, boolean keepFirst) {
        if (keepFirst)
            return def.split("\n   AND ")[0] + ";";
        else
            return def.split(" WITH ")[0] + ";";
    }

    // Note: this test is a bit fragile in the sense that it rely on the exact
    // string formatting of exportAsString, but it's a very simple/convenient
    // way to check we correctly handle schemas so it's probably not so bad.
    // In particular, exportAsString *does not* guarantee that you'll get
    // exactly the same string than the one used to create the table.
    @Test(groups = "integration")
    public void schemaExportTest() {

        KeyspaceMetadata metadata = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE);

        for (Map.Entry<String, String> tableEntry : cql3.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(stripOptions(metadata.getTable(table).exportAsString(), false), def);
        }

        for (Map.Entry<String, String> tableEntry : compact.entrySet()) {
            String table = tableEntry.getKey();
            String def = tableEntry.getValue();
            assertEquals(stripOptions(metadata.getTable(table).exportAsString(), true), def);
        }
    }

    // Same remark as the preceding test
    @Test(groups = "integration")
    public void schemaExportOptionsTest() {
        TableMetadata metadata = cluster.getMetadata().getKeyspace(TestUtils.SIMPLE_KEYSPACE).getTable("with_options");
        assertEquals(metadata.exportAsString(), withOptions);
    }
}
