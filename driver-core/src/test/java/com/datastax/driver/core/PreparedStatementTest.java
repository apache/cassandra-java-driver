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

import com.datastax.driver.core.exceptions.*;
import static com.datastax.driver.core.TestUtils.*;

/**
 * Prepared statement tests.
 *
 * Note: this class also happens to test all the get methods from Row.
 */
public class PreparedStatementTest extends CCMBridge.PerClassSingleNodeCluster {

    private static final String ALL_NATIVE_TABLE = "all_native";
    private static final String ALL_LIST_TABLE = "all_list";
    private static final String ALL_SET_TABLE = "all_set";
    private static final String ALL_MAP_TABLE = "all_map";
    private static final String SIMPLE_TABLE = "test";

    private boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    protected Collection<String> getTableDefinitions() {

        List<String> defs = new ArrayList<String>(4);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_NATIVE_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            if (exclude(type))
                continue;
            sb.append(", c_").append(type).append(" ").append(type);
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_LIST_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            if (exclude(type))
                continue;
            sb.append(", c_list_").append(type).append(" list<").append(type).append(">");
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_SET_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(type))
                continue;
            sb.append(", c_set_").append(type).append(" set<").append(type).append(">");
        }
        sb.append(")");
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_MAP_TABLE).append(" (k text PRIMARY KEY");
        for (DataType keyType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(keyType))
                continue;

            for (DataType valueType : DataType.allPrimitiveTypes()) {
                // This must be handled separatly
                if (exclude(valueType))
                    continue;
                sb.append(", c_map_").append(keyType).append("_").append(valueType).append(" map<").append(keyType).append(",").append(valueType).append(">");
            }
        }
        sb.append(")");
        defs.add(sb.toString());

        defs.add(String.format("CREATE TABLE %s (k text PRIMARY KEY, i int)", SIMPLE_TABLE));

        return defs;
    }

    @Test(groups = "integration")
    public void preparedNativeTest() {
        // Test preparing/bounding for all native types
        for (DataType type : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(type))
                continue;

            String name = "c_" + type;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_native', ?)", ALL_NATIVE_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, getFixedValue(type)));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_native'", name, ALL_NATIVE_TABLE)).one();
            assertEquals(getValue(row, name, type), getFixedValue(type), "For type " + type);
        }
    }

    /**
     * Almost the same as preparedNativeTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "integration")
    public void preparedNativeTest2() {
        // Test preparing/bounding for all native types
        for (DataType type : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(type))
                continue;

            String name = "c_" + type;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_native', ?)", ALL_NATIVE_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, getFixedValue2(type)));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_native'", name, ALL_NATIVE_TABLE)).one();
            assertEquals(getValue(row, name, type), getFixedValue2(type), "For type " + type);
        }
    }

    @Test(groups = "integration")
    public void prepareListTest() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List value = (List)getFixedValue(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    /**
     * Almost the same as prepareListTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "integration")
    public void prepareListTest2() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List value = (List)getFixedValue2(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    @Test(groups = "integration")
    public void prepareSetTest() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set value = (Set)getFixedValue(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    /**
     * Almost the same as prepareSetTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "integration")
    public void prepareSetTest2() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set value = (Set)getFixedValue2(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    @Test(groups = "integration")
    public void prepareMapTest() {
        // Test preparing/bounding for all possible map types
        for (DataType rawKeyType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawKeyType))
                continue;

            for (DataType rawValueType : DataType.allPrimitiveTypes()) {
                // This must be handled separatly
                if (exclude(rawValueType))
                    continue;

                String name = "c_map_" + rawKeyType + "_" + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map value = (Map)getFixedValue(type);;
                PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.bind();
                session.execute(setBoundValue(bs, name, type, value));

                Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).one();
                assertEquals(getValue(row, name, type), value, "For type " + type);
            }
        }
    }

    /**
     * Almost the same as prepareMapTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "integration")
    public void prepareMapTest2() {
        // Test preparing/bounding for all possible map types
        for (DataType rawKeyType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawKeyType))
                continue;

            for (DataType rawValueType : DataType.allPrimitiveTypes()) {
                // This must be handled separatly
                if (exclude(rawValueType))
                    continue;

                String name = "c_map_" + rawKeyType + "_" + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map value = (Map)getFixedValue2(type);;
                PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.bind();
                session.execute(setBoundValue(bs, name, type, value));

                Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).one();
                assertEquals(getValue(row, name, type), value, "For type " + type);
            }
        }
    }

    private void reprepareOnNewlyUpNodeTest(String ks, Session session) throws Exception {

        ks = ks == null ? "" : ks + ".";

        session.execute("INSERT INTO " + ks + "test (k, i) VALUES ('123', 17)");
        session.execute("INSERT INTO " + ks + "test (k, i) VALUES ('124', 18)");

        PreparedStatement ps = session.prepare("SELECT * FROM " + ks + "test WHERE k = ?");

        assertEquals(session.execute(ps.bind("123")).one().getInt("i"), 17);

        cassandraCluster.stop();
        // We have one node, so if we shut it down and do nothing, the driver
        // won't notice the node is dead (until keep alive kicks in at least,
        // but that's a fairly long time). So we cheat and just do any request
        // to force the detection.
        cluster.manager.submitSchemaRefresh(null, null);
        waitForDown(CCMBridge.IP_PREFIX + "1", cluster, 20);

        cassandraCluster.start();
        waitFor(CCMBridge.IP_PREFIX + "1", cluster, 20);

        try
        {
            assertEquals(session.execute(ps.bind("124")).one().getInt("i"), 18);
        }
        catch (NoHostAvailableException e)
        {
            System.out.println(">> " + e.getErrors());
            throw e;
        }
    }

    @Test(groups = "integration")
    public void reprepareOnNewlyUpNodeTest() throws Exception {
        reprepareOnNewlyUpNodeTest(null, session);
    }

    @Test(groups = "integration")
    public void reprepareOnNewlyUpNodeNoKeyspaceTest() throws Exception {

        // This is the same test than reprepareOnNewlyUpNodeTest, except that the
        // prepared statement is prepared while no current keyspace is used
        reprepareOnNewlyUpNodeTest(TestUtils.SIMPLE_KEYSPACE, cluster.connect());
    }

    /**
     * Prints the table definitions that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = { "docs" })
    public void printTableDefinitions() {
        for (String definition : getTableDefinitions()) {
            System.out.println(definition);
        }
    }
}
