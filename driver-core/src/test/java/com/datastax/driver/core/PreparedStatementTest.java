/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.driver.core.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.*;

/**
 * Prepared statement tests.
 * <p/>
 * Note: this class also happens to test all the get methods from Row.
 */
@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
public class PreparedStatementTest extends CCMTestsSupport {

    private static final String ALL_NATIVE_TABLE = "all_native";
    private static final String ALL_LIST_TABLE = "all_list";
    private static final String ALL_SET_TABLE = "all_set";
    private static final String ALL_MAP_TABLE = "all_map";
    private static final String SIMPLE_TABLE = "test";
    private static final String SIMPLE_TABLE2 = "test2";

    private ProtocolVersion protocolVersion;
    private Collection<DataType> primitiveTypes;

    private boolean exclude(DataType t) {
        // duration is not supported in collections
        return t.getName() == DataType.Name.COUNTER || t.getName() == DataType.Name.DURATION;
    }

    @Override
    public void onTestContextInitialized() {
        protocolVersion = ccm().getProtocolVersion();
        primitiveTypes = TestUtils.allPrimitiveTypes(protocolVersion);
        execute(createTestFixtures());
    }

    private List<String> createTestFixtures() {
        List<String> defs = new ArrayList<String>(4);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_NATIVE_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : primitiveTypes) {
            if (exclude(type))
                continue;
            sb.append(", c_").append(type).append(' ').append(type);
        }
        sb.append(')');
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_LIST_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : primitiveTypes) {
            if (exclude(type))
                continue;
            sb.append(", c_list_").append(type).append(" list<").append(type).append('>');
        }
        sb.append(')');
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_SET_TABLE).append(" (k text PRIMARY KEY");
        for (DataType type : primitiveTypes) {
            // This must be handled separately
            if (exclude(type))
                continue;
            sb.append(", c_set_").append(type).append(" set<").append(type).append('>');
        }
        sb.append(')');
        defs.add(sb.toString());

        sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(ALL_MAP_TABLE).append(" (k text PRIMARY KEY");
        for (DataType keyType : primitiveTypes) {
            // This must be handled separately
            if (exclude(keyType))
                continue;

            for (DataType valueType : primitiveTypes) {
                // This must be handled separately
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
        for (DataType type : primitiveTypes) {
            // This must be handled separately
            if (exclude(type))
                continue;

            String name = "c_" + type;
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_native', ?)", ALL_NATIVE_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, getFixedValue(type));
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_native'", name, ALL_NATIVE_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), getFixedValue(type), "For type " + type);
        }
    }

    /**
     * Almost the same as preparedNativeTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "short")
    public void preparedNativeTest2() {
        // Test preparing/bounding for all native types
        for (DataType type : primitiveTypes) {
            // This must be handled separately
            if (exclude(type))
                continue;

            String name = "c_" + type;
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_native', ?)", ALL_NATIVE_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, getFixedValue2(type));
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_native'", name, ALL_NATIVE_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), getFixedValue2(type), "For type " + type);
        }
    }

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareListTest() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List<Object> value = (List<Object>) getFixedValue(type);
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, value);
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
        }
    }

    /**
     * Almost the same as prepareListTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareListTest2() {
        // Test preparing/bounding for all possible list types
        for (DataType rawType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawType))
                continue;

            String name = "c_list_" + rawType;
            DataType type = DataType.list(rawType);
            List<Object> value = (List<Object>) getFixedValue2(type);
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_list', ?)", ALL_LIST_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, value);
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_list'", name, ALL_LIST_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
        }
    }

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareSetTest() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set<Object> value = (Set<Object>) getFixedValue(type);
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, value);
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
        }
    }

    /**
     * Almost the same as prepareSetTest, but it uses getFixedValue2() instead.
     */
    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareSetTest2() {
        // Test preparing/bounding for all possible set types
        for (DataType rawType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawType))
                continue;

            String name = "c_set_" + rawType;
            DataType type = DataType.set(rawType);
            Set<Object> value = (Set<Object>) getFixedValue2(type);
            PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_set', ?)", ALL_SET_TABLE, name));
            BoundStatement bs = ps.bind();
            setValue(bs, name, type, value);
            session().execute(bs);

            Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_set'", name, ALL_SET_TABLE)).one();
            assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
        }
    }

    @Test(groups = "short")
    @SuppressWarnings("unchecked")
    public void prepareMapTest() {
        // Test preparing/bounding for all possible map types
        for (DataType rawKeyType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawKeyType))
                continue;

            for (DataType rawValueType : primitiveTypes) {
                // This must be handled separately
                if (exclude(rawValueType))
                    continue;

                String name = "c_map_" + rawKeyType + '_' + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map<Object, Object> value = (Map<Object, Object>) getFixedValue(type);
                PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.bind();
                setValue(bs, name, type, value);
                session().execute(bs);

                Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).one();
                assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
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
        for (DataType rawKeyType : primitiveTypes) {
            // This must be handled separately
            if (exclude(rawKeyType))
                continue;

            for (DataType rawValueType : primitiveTypes) {
                // This must be handled separately
                if (exclude(rawValueType))
                    continue;

                String name = "c_map_" + rawKeyType + '_' + rawValueType;
                DataType type = DataType.map(rawKeyType, rawValueType);
                Map<Object, Object> value = (Map<Object, Object>) getFixedValue2(type);
                PreparedStatement ps = session().prepare(String.format("INSERT INTO %s(k, %s) VALUES ('prepared_map', ?)", ALL_MAP_TABLE, name));
                BoundStatement bs = ps.bind();
                setValue(bs, name, type, value);
                session().execute(bs);

                Row row = session().execute(String.format("SELECT %s FROM %s WHERE k='prepared_map'", name, ALL_MAP_TABLE)).one();
                assertEquals(getValue(row, name, type, cluster().getConfiguration().getCodecRegistry()), value, "For type " + type);
            }
        }
    }

    @Test(groups = "short")
    public void prepareWithNullValuesTest() throws Exception {

        PreparedStatement ps = session().prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, ?)");

        session().execute(ps.bind("prepWithNull1", null));

        BoundStatement bs = ps.bind();
        bs.setString("k", "prepWithNull2");
        bs.setString("v", null);
        session().execute(bs);

        ResultSet rs = session().execute("SELECT * FROM " + SIMPLE_TABLE2 + " WHERE k IN ('prepWithNull1', 'prepWithNull2')");
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
        toPrepare.setSerialConsistencyLevel(ConsistencyLevel.LOCAL_SERIAL);
        toPrepare.setRetryPolicy(FallthroughRetryPolicy.INSTANCE);
        if (protocolVersion.compareTo(V4) >= 0)
            toPrepare.setOutgoingPayload(ImmutableMap.of("foo", Bytes.fromHexString("0xcafebabe")));
        toPrepare.setIdempotent(true);
        toPrepare.enableTracing();

        PreparedStatement prepared = session().prepare(toPrepare);
        assertThat(prepared.getConsistencyLevel()).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(prepared.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
        assertThat(prepared.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
        if (protocolVersion.compareTo(V4) >= 0)
            assertThat(prepared.getOutgoingPayload()).isEqualTo(ImmutableMap.of("foo", Bytes.fromHexString("0xcafebabe")));
        assertThat(prepared.isIdempotent()).isTrue();
        assertThat(prepared.isTracing()).isTrue();

        BoundStatement bs = prepared.bind("someValue");
        assertThat(bs.getConsistencyLevel()).isEqualTo(ConsistencyLevel.QUORUM);
        assertThat(bs.getSerialConsistencyLevel()).isEqualTo(ConsistencyLevel.LOCAL_SERIAL);
        assertThat(bs.getRetryPolicy()).isEqualTo(FallthroughRetryPolicy.INSTANCE);
        if (protocolVersion.compareTo(V4) >= 0)
            assertThat(bs.getOutgoingPayload()).isEqualTo(ImmutableMap.of("foo", Bytes.fromHexString("0xcafebabe")));
        assertThat(bs.isIdempotent()).isTrue();
        assertThat(bs.isTracing()).isTrue();
    }

    /**
     * Prints the table definitions that will be used in testing
     * (for exporting purposes)
     */
    @Test(groups = {"docs"})
    public void printTableDefinitions() {
        for (String definition : createTestFixtures()) {
            System.out.println(definition);
        }
    }

    @Test(groups = "short")
    public void batchTest() throws Exception {

        try {
            PreparedStatement ps1 = session().prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, ?)");
            PreparedStatement ps2 = session().prepare("INSERT INTO " + SIMPLE_TABLE2 + "(k, v) VALUES (?, 'bar')");

            BatchStatement bs = new BatchStatement();
            bs.add(ps1.bind("one", "foo"));
            bs.add(ps2.bind("two"));
            bs.add(new SimpleStatement("INSERT INTO " + SIMPLE_TABLE2 + " (k, v) VALUES ('three', 'foobar')"));

            session().execute(bs);

            List<Row> all = session().execute("SELECT * FROM " + SIMPLE_TABLE2).all();

            assertEquals("three", all.get(0).getString("k"));
            assertEquals("foobar", all.get(0).getString("v"));

            assertEquals("one", all.get(1).getString("k"));
            assertEquals("foo", all.get(1).getString("v"));

            assertEquals("two", all.get(2).getString("k"));
            assertEquals("bar", all.get(2).getString("v"));
        } catch (UnsupportedFeatureException e) {
            // This is expected when testing the protocol v1
            if (cluster().getConfiguration().getProtocolOptions().getProtocolVersion() != ProtocolVersion.V1)
                throw e;
        }
    }

    @Test(groups = "short")
    public void should_set_routing_key_on_case_insensitive_keyspace_and_table() {
        session().execute(String.format("CREATE TABLE %s.foo (i int PRIMARY KEY)", keyspace));

        PreparedStatement ps = session().prepare(String.format("INSERT INTO %s.foo (i) VALUES (?)", keyspace));
        BoundStatement bs = ps.bind(1);
        assertThat(bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNotNull();
    }

    @Test(groups = "short")
    public void should_set_routing_key_on_case_sensitive_keyspace_and_table() {
        session().execute("CREATE KEYSPACE \"Test\" WITH replication = { "
                + "  'class': 'SimpleStrategy',"
                + "  'replication_factor': '1'"
                + "}");
        session().execute("CREATE TABLE \"Test\".\"Foo\" (i int PRIMARY KEY)");

        PreparedStatement ps = session().prepare("INSERT INTO \"Test\".\"Foo\" (i) VALUES (?)");
        BoundStatement bs = ps.bind(1);
        assertThat(bs.getRoutingKey(ProtocolVersion.NEWEST_SUPPORTED, CodecRegistry.DEFAULT_INSTANCE)).isNotNull();
    }

    @Test(groups = "short", expectedExceptions = InvalidQueryException.class)
    public void should_fail_when_prepared_on_another_cluster() throws Exception {
        Cluster otherCluster = Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build();
        try {
            PreparedStatement pst = otherCluster.connect().prepare("select * from system.peers where inet = ?");
            BoundStatement bs = pst.bind().setInet(0, InetAddress.getByName("localhost"));

            // We expect that the error gets detected without a roundtrip to the server, so use executeAsync
            session().executeAsync(bs);
        } finally {
            otherCluster.close();
        }
    }

    /**
     * Tests that, under protocol versions lesser than V4,
     * it is NOT possible to execute a prepared statement with unbound values.
     * Note that we have to force protocol version to less than V4 because
     * higher protocol versions would allow such unbound values to be sent.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "short")
    public void should_not_allow_unbound_value_on_bound_statement_when_protocol_lesser_than_v4() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(ccm().getProtocolVersion(ProtocolVersion.V3))
                .build());
        Session session = cluster.connect();
        try {
            PreparedStatement ps = session.prepare("INSERT INTO " + keyspace + "." + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
            BoundStatement bs = ps.bind("foo");
            assertFalse(bs.isSet("i"));
            session.execute(bs);
            fail("Should not have executed statement with UNSET values in protocol V3");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("Unset value at index 1");
        }
    }

    /**
     * Tests that, under protocol versions lesser that V4,
     * it is NOT possible to execute a prepared statement with unbound values.
     * Note that we have to force protocol version to less than V4 because
     * higher protocol versions would allow such unbound values to be sent.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_not_allow_unbound_value_on_batch_statement_when_protocol_lesser_than_v4() {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withProtocolVersion(ccm().getProtocolVersion(ProtocolVersion.V3))
                .build());
        Session session = cluster.connect();
        try {
            PreparedStatement ps = session.prepare("INSERT INTO " + keyspace + "." + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
            BatchStatement batch = new BatchStatement();
            batch.add(ps.bind("foo"));
            // i is UNSET
            session.execute(batch);
            fail("Should not have executed statement with UNSET values in protocol V3");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage()).contains("Unset value at index 1");
        }
    }

    /**
     * Tests that a tombstone is NOT created when a column in a prepared statement
     * is not bound (UNSET flag).
     * This only works from protocol V4 onwards.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_not_create_tombstone_when_unbound_value_on_bound_statement_and_protocol_v4() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement st1 = prepared.bind();
        st1.setString(0, "foo");
        st1.setInt(1, 1234);
        session().execute(st1);
        BoundStatement st2 = prepared.bind();
        st2.setString(0, "foo");
        // i is UNSET
        session().execute(st2);
        Statement st3 = new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'");
        st3.enableTracing();
        ResultSet rows = session().execute(st3);
        assertThat(rows.one().getInt("i")).isEqualTo(1234);
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "0 tombstone");
    }

    /**
     * Tests that a value that was previously set on a bound statement can be unset by index.
     * This only works from protocol V4 onwards.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-930
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_unset_value_by_index() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement bound = prepared.bind();
        bound.setString(0, "foo");
        bound.setInt(1, 1234);

        bound.unset(1);
        assertThat(bound.isSet(1)).isFalse();
        session().execute(bound);

        ResultSet rows = session().execute(
                new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'")
                        .enableTracing());

        assertThat(rows.one().isNull("i"));
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "0 tombstone");
    }

    /**
     * Tests that a value that was previously set on a bound statement can be unset by name.
     * This only works from protocol V4 onwards.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-930
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_unset_value_by_name() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (:k, :i)");
        BoundStatement bound = prepared.bind();
        bound.setString("k", "foo");
        bound.setInt("i", 1234);

        bound.unset("i");
        assertThat(bound.isSet("i")).isFalse();
        session().execute(bound);

        ResultSet rows = session().execute(
                new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'")
                        .enableTracing());

        assertThat(rows.one().isNull("i"));
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "0 tombstone");
    }

    /**
     * Tests that a tombstone is NOT created when a column in a prepared statement
     * is not bound (UNSET flag).
     * This only works from protocol V4 onwards.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_not_create_tombstone_when_unbound_value_on_batch_statement_and_protocol_v4() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement st1 = prepared.bind();
        st1.setString(0, "foo");
        st1.setInt(1, 1234);
        session().execute(new BatchStatement().add(st1));
        BoundStatement st2 = prepared.bind();
        st2.setString(0, "foo");
        // i is UNSET
        session().execute(new BatchStatement().add(st2));
        Statement st3 = new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'");
        st3.enableTracing();
        ResultSet rows = session().execute(st3);
        assertThat(rows.one().getInt("i")).isEqualTo(1234);
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "0 tombstone");
    }

    /**
     * Tests that a tombstone is created when binding a null value to a column in a prepared statement.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "long")
    public void should_create_tombstone_when_null_value_on_bound_statement() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement st1 = prepared.bind();
        st1.setString(0, "foo");
        st1.setToNull(1);
        session().execute(st1);
        Statement st2 = new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'");
        st2.enableTracing();
        ResultSet rows = session().execute(st2);
        assertThat(rows.one().isNull(0)).isTrue();
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "1 tombstone");
    }

    /**
     * Tests that a tombstone is created when binding a null value to a column in a batch statement.
     *
     * @test_category prepared_statements:binding
     * @jira_ticket JAVA-777
     * @since 2.2.0
     */
    @Test(groups = "short")
    @CassandraVersion("2.0.0")
    public void should_create_tombstone_when_null_value_on_batch_statement() {
        PreparedStatement prepared = session().prepare("INSERT INTO " + SIMPLE_TABLE + " (k, i) VALUES (?, ?)");
        BoundStatement st1 = prepared.bind();
        st1.setString(0, "foo");
        st1.setToNull(1);
        session().execute(new BatchStatement().add(st1));
        Statement st2 = new SimpleStatement("SELECT i from " + SIMPLE_TABLE + " where k = 'foo'");
        st2.enableTracing();
        ResultSet rows = session().execute(st2);
        assertThat(rows.one().isNull(0)).isTrue();
        // sleep 10 seconds to make sure the trace will be complete
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
        QueryTrace queryTrace = rows.getExecutionInfo().getQueryTrace();
        assertEventsContain(queryTrace, "1 tombstone");
    }

    private void assertEventsContain(QueryTrace queryTrace, String toFind) {
        for (QueryTrace.Event event : queryTrace.getEvents()) {
            if (event.getDescription().contains(toFind))
                return;
        }
        fail("Did not find '" + toFind + "' in trace");
    }

    @Test(groups = "short")
    public void should_propagate_idempotence_in_statements() {
        session().execute(String.format("CREATE TABLE %s.idempotencetest (i int PRIMARY KEY)", keyspace));

        SimpleStatement statement;
        PreparedStatement prepared;
        BoundStatement bound;

        statement = new SimpleStatement(String.format("SELECT * FROM %s.idempotencetest WHERE i = ?", keyspace));

        prepared = session().prepare(statement);
        bound = prepared.bind(1);

        assertThat(prepared.isIdempotent()).isNull();
        assertThat(bound.isIdempotent()).isNull();

        statement.setIdempotent(true);
        prepared = session().prepare(statement);
        bound = prepared.bind(1);

        assertThat(prepared.isIdempotent()).isTrue();
        assertThat(bound.isIdempotent()).isTrue();

        statement.setIdempotent(false);
        prepared = session().prepare(statement);
        bound = prepared.bind(1);

        assertThat(prepared.isIdempotent()).isFalse();
        assertThat(bound.isIdempotent()).isFalse();

        prepared.setIdempotent(true);
        bound = prepared.bind(1);

        assertThat(bound.isIdempotent()).isTrue();
    }
}
