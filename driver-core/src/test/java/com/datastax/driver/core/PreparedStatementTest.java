/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import static org.testng.Assert.assertFalse;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.utils.CassandraVersion;

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
    private static final String SIMPLE_TABLE2 = "test2";

    private boolean exclude(DataType t) {
        return t.getName() == DataType.Name.COUNTER;
    }

    @Override
    protected Collection<String> getTableDefinitions() {

        List<String> defs = new ArrayList<String>(4);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_NATIVE_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            if (exclude(type))
                continue;
            sb.append(", c_").append(type).append(' ').append(type);
        }
        sb.append(')');
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_LIST_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            if (exclude(type))
                continue;
            sb.append(", c_list_").append(type).append(" list<").append(type).append('>');
        }
        sb.append(')');
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_SET_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(type))
                continue;
            sb.append(", c_set_").append(type).append(" set<").append(type).append('>');
        }
        sb.append(')');
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
                sb.append(", c_map_").append(keyType).append('_').append(valueType).append(" map<").append(keyType).append(',').append(valueType).append('>');
            }
        }
        sb.append(')');
        defs.add(sb.toString());

        defs.add(String.format("CREATE TABLE %s (k text PRIMARY KEY, i int)", SIMPLE_TABLE));
        defs.add(String.format("CREATE TABLE %s (k text PRIMARY KEY, v text)", SIMPLE_TABLE2));

        return defs;
    }

    @Test(groups = "short")
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
    @Test(groups = "short")
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

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareListTest() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List<Object> value = (List<Object>)getFixedValue(type);;
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
    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareListTest2() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List<Object> value = (List<Object>)getFixedValue2(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareSetTest() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set<Object> value = (Set<Object>)getFixedValue(type);;
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
    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareSetTest2() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : DataType.allPrimitiveTypes()) {
            // This must be handled separatly
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set<Object> value = (Set<Object>)getFixedValue2(type);;
            PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.bind();
            session.execute(setBoundValue(bs, name, type, value));

            Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).one();
            assertEquals(getValue(row, name, type), value, "For type " + type);
        }
    }

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
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

                String name = "c_map_" + rawKeyType + '_' + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map<Object, Object> value = (Map<Object, Object>)getFixedValue(type);;
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
    @Test(groups = "short")
    @SuppressWarnings("unchecked")
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

                String name = "c_map_" + rawKeyType + '_' + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map<Object, Object> value = (Map<Object, Object>)getFixedValue2(type);;
                PreparedStatement ps = session.prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.bind();
                session.execute(setBoundValue(bs, name, type, value));

                Row row = session.execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).one();
                assertEquals(getValue(row, name, type), value, "For type " + type);
            }
        }
    }

    private void reprepareOnNewlyUpNodeTest(String ks, Session session) throws Exception {

        ks = ks == null ? "" : ks + '.';

        session.execute("INSERT INTO " + ks + "test (k, i) VALUES ('123', 17)");
        session.execute("INSERT INTO " + ks + "test (k, i) VALUES ('124', 18)");

        PreparedStatement ps = session.prepare("SELECT * FROM " + ks + "test WHERE k = ?");

        assertEquals(session.execute(ps.bind("123")).one().getInt("i"), 17);

        cassandraCluster.stop();
        waitForDown(CCMBridge.IP_PREFIX + '1', cluster);

        cassandraCluster.start();
        waitFor(CCMBridge.IP_PREFIX + '1', cluster, 120);

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

    @Test(groups = "long")
    public void reprepareOnNewlyUpNodeTest() throws Exception {
        reprepareOnNewlyUpNodeTest(null, session);
    }

    @Test(groups = "long")
    public void reprepareOnNewlyUpNodeNoKeyspaceTest() throws Exception {

        // This is the same test than reprepareOnNewlyUpNodeTest, except that the
        // prepared statement is prepared while no current keyspace is used
        reprepareOnNewlyUpNodeTest(TestUtils.SIMPLE_KEYSPACE, cluster.connect());
    }

    @Test(groups = "short")
    public void prepareWithNullValuesTest() throws Exception {

        PreparedStatement ps = session.prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, ?)");

        session.execute(ps.bind("prepWithNull1", null));

        BoundStatement bs = ps.bind();
        bs.setString("k", "prepWithNull2");
        bs.setString("v", null);
        session.execute(bs);

        ResultSet rs = session.execute("SELECT * FROM " + SIMPLE_TABLE2 + " WHERE k IN ('prepWithNull1', 'prepWithNull2')");
        Row r1 = rs.one();
        Row r2 = rs.one();
        assertTrue(rs.isExhausted());

        assertEquals(r1.getString("k"), "prepWithNull1");
        assertEquals(r1.getString("v"), null);

        assertEquals(r2.getString("k"), "prepWithNull2");
        assertEquals(r2.getString("v"), null);
    }

    @Test(groups = "short")
    public void prepareStatementInheritPropertiesTest() {

        RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?");
        toPrepare.setConsistencyLevel(ConsistencyLevel.QUORUM);
        toPrepare.enableTracing();

        PreparedStatement prepared = session.prepare(toPrepare);
        BoundStatement bs = prepared.bind("someValue");

        assertEquals(ConsistencyLevel.QUORUM, bs.getConsistencyLevel());
        assertTrue(bs.isTracing());
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

    @Test(groups = "short")
    public void batchTest() throws Exception {

        try {
            PreparedStatement ps1 = session.prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, ?)");
            PreparedStatement ps2 = session.prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, 'bar')");

            BatchStatement bs = new BatchStatement();
            bs.add(ps1.bind("one", "foo"));
            bs.add(ps2.bind("two"));
            bs.add(new SimpleStatement("INSERT INTO " + SIMPLE_TABLE2 + " (k, v) VALUES ('three', 'foobar')"));

            session.execute(bs);

            List<Row> all = session.execute("SELECT * FROM " + SIMPLE_TABLE2).all();

            assertEquals("three", all.get(0).getString("k"));
            assertEquals("foobar", all.get(0).getString("v"));

            assertEquals("one", all.get(1).getString("k"));
            assertEquals("foo", all.get(1).getString("v"));

            assertEquals("two", all.get(2).getString("k"));
            assertEquals("bar", all.get(2).getString("v"));
        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            if (cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum() != ProtocolVersion.V1)
                throw e;
        }
    }

    @Test(groups = "short", expectedExceptions = { IllegalStateException.class })
    public void unboundVariableInBoundStatementTest() {
        PreparedStatement ps = session.prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement bs = ps.bind("k");
        assertFalse(bs.isSet("i"));
        session.execute(bs);
    }

    @Test(groups = "short", expectedExceptions = { IllegalStateException.class })
    @CassandraVersion(major=2.0)
    public void unboundVariableInBatchStatementTest() {
        PreparedStatement ps = session.prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BatchStatement batch = new BatchStatement();
        batch.add(ps.bind("k"));
        session.execute(batch);
    }

    @Test(groups="short")
    public void should_set_routing_key_on_case_insensitive_keyspace_and_table() {
        session.execute("CREATE TABLE ks.foo (i int PRIMARY KEY)");

        PreparedStatement ps = session.prepare("INSERT INTO ks.foo (i) VALUES (?)");
        BoundStatement bs = ps.bind(1);
        assertThat(bs.getRoutingKey()).isNotNull();
    }

    @Test(groups="short")
    public void should_set_routing_key_on_case_sensitive_keyspace_and_table() {
        session.execute("CREATE KEYSPACE \"Test\" WITH replication = { "
            + "  'class': 'SimpleStrategy',"
            + "  'replication_factor': '1'"
            + "}");
        session.execute("CREATE TABLE \"Test\".\"Foo\" (i int PRIMARY KEY)");

        PreparedStatement ps = session.prepare("INSERT INTO \"Test\".\"Foo\" (i) VALUES (?)");
        BoundStatement bs = ps.bind(1);
        assertThat(bs.getRoutingKey()).isNotNull();
    }
}
