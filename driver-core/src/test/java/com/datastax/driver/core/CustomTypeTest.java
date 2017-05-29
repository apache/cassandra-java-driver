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

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.custom;
import static com.datastax.driver.core.TestUtils.serializeForCompositeType;
import static com.datastax.driver.core.TestUtils.serializeForDynamicCompositeType;

/**
 * Test we "support" custom types.
 */
public class CustomTypeTest extends CCMTestsSupport {

    public static final DataType CUSTOM_DYNAMIC_COMPOSITE = custom(
            "org.apache.cassandra.db.marshal.DynamicCompositeType("
                    + "s=>org.apache.cassandra.db.marshal.UTF8Type,"
                    + "i=>org.apache.cassandra.db.marshal.Int32Type)");

    public static final DataType CUSTOM_COMPOSITE = custom(
            "org.apache.cassandra.db.marshal.CompositeType("
                    + "org.apache.cassandra.db.marshal.UTF8Type,"
                    + "org.apache.cassandra.db.marshal.Int32Type)");

    @Override
    public void onTestContextInitialized() {
        execute(
                "CREATE TABLE test ("
                        + "    k int,"
                        + "    c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                        + "    c2 'ReversedType(CompositeType(UTF8Type, Int32Type))'," // reversed translates to CLUSTERING ORDER BY DESC
                        + "    c3 'Int32Type'," // translates to int
                        + "    PRIMARY KEY (k, c1, c2)"
                        + ") WITH COMPACT STORAGE",
                "CREATE TABLE test_collection("
                        + "    k int PRIMARY KEY,"
                        + "    c1 list<'DynamicCompositeType(s => UTF8Type, i => Int32Type)'>,"
                        + "    c2 map<'DynamicCompositeType(s => UTF8Type, i => Int32Type)', 'DynamicCompositeType(s => UTF8Type, i => Int32Type)'>"
                        + ")"
        );
    }

    /**
     * Validates that columns using custom types are properly handled by the driver in the following ways:
     * <p/>
     * <ol>
     * <li>The column metadata appropriately represents the types as {@link DataType#custom(String)}</li>
     * <li>ReversedType is appropriately detected and the clustering order of that column is marked as descending.</li>
     * <li>ColumnDefinitions for a column in a {@link Row} matches the custom type and that inserted data is read back properly.</li>
     * </ol>
     *
     * @jira_ticket JAVA-993
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_serialize_and_deserialize_custom_types() {

        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("test");

        assertThat(table.getColumn("c1")).isClusteringColumn().hasType(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(table.getColumn("c2")).isClusteringColumn().hasType(CUSTOM_COMPOSITE).hasClusteringOrder(ClusteringOrder.DESC);
        assertThat(table.getColumn("c3")).hasType(cint());

        session().execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 's@foo:i@32', 'foo:32', 1)");
        session().execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@42', ':42', 2)");
        session().execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@12:i@3', 'foo', 3)");

        ResultSet rs = session().execute("SELECT * FROM test");

        Row r = rs.one();

        assertThat(r.getColumnDefinitions().getType("c1")).isEqualTo(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(r.getColumnDefinitions().getType("c2")).isEqualTo(CUSTOM_COMPOSITE);
        assertThat(r.getColumnDefinitions().getType("c3")).isEqualTo(cint());

        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType(12, 3));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("foo"));
        assertThat(r.getInt("c3")).isEqualTo(3);

        r = rs.one();
        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType(42));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("", 42));
        assertThat(r.getInt("c3")).isEqualTo(2);

        r = rs.one();
        assertThat(r.getInt("k")).isEqualTo(0);
        assertThat(r.getBytesUnsafe("c1")).isEqualTo(serializeForDynamicCompositeType("foo", 32));
        assertThat(r.getBytesUnsafe("c2")).isEqualTo(serializeForCompositeType("foo", 32));
        assertThat(r.getInt("c3")).isEqualTo(1);
    }

    /**
     * Validates that columns using collections of custom types are properly handled by the driver.
     *
     * @jira_ticket JAVA-1034
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_serialize_and_deserialize_collections_of_custom_types() {
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("test_collection");
        assertThat(table.getColumn("c1")).hasType(DataType.list(CUSTOM_DYNAMIC_COMPOSITE));
        assertThat(table.getColumn("c2")).hasType(DataType.map(CUSTOM_DYNAMIC_COMPOSITE, CUSTOM_DYNAMIC_COMPOSITE));

        session().execute("INSERT INTO test_collection(k, c1, c2) VALUES (0, [ 's@foo:i@32' ], { 's@foo:i@32': 's@bar:i@42' })");

        Row r = session().execute("SELECT * FROM test_collection").one();

        assertThat(r.getColumnDefinitions().getType("c1")).isEqualTo(DataType.list(CUSTOM_DYNAMIC_COMPOSITE));
        List<ByteBuffer> c1 = r.getList("c1", ByteBuffer.class);
        assertThat(c1.get(0)).isEqualTo(serializeForDynamicCompositeType("foo", 32));

        assertThat(r.getColumnDefinitions().getType("c2")).isEqualTo(DataType.map(CUSTOM_DYNAMIC_COMPOSITE, CUSTOM_DYNAMIC_COMPOSITE));
        Map<ByteBuffer, ByteBuffer> c2 = r.getMap("c2", ByteBuffer.class, ByteBuffer.class);
        Map.Entry<ByteBuffer, ByteBuffer> entry = c2.entrySet().iterator().next();
        assertThat(entry.getKey()).isEqualTo(serializeForDynamicCompositeType("foo", 32));
        assertThat(entry.getValue()).isEqualTo(serializeForDynamicCompositeType("bar", 42));
    }

    /**
     * Validates that UDTs with fields using custom types are properly handled by the driver in the following ways:
     * <p/>
     * <ol>
     * <li>The {@link UserType} metadata appropriately represents the types of fields with custom types as {@link DataType#custom(String)}</li>
     * <li>{@link TableMetadata} with a column having a {@link UserType} is properly referenced.</li>
     * <li>ColumnDefinitions for a column in a {@link Row} matches the {@link UserType} and that inserted data is read back properly.</li>
     * </ol>
     *
     * @jira_ticket JAVA-993
     * @test_category metadata
     */
    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_handle_udt_with_custom_type() {
        // Given: a UDT with custom types, and a table using it.
        session().execute("CREATE TYPE custom_udt (regular int, c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)', c2 'LongType')");
        session().execute("CREATE TABLE custom_udt_tbl (k int primary key, v frozen<custom_udt>)");

        // When: Retrieving User Type via schema metadata.
        UserType custom_udt = cluster().getMetadata().getKeyspace(keyspace).getUserType("custom_udt");
        assertThat(custom_udt.getFieldType("regular")).isEqualTo(cint());
        // Then: The fields with custom types should be appropriately represented with their defined types.
        assertThat(custom_udt.getFieldType("c1")).isEqualTo(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(custom_udt.getFieldType("c2")).isEqualTo(DataType.bigint());

        // When: Retrieving Table via schema metadata.
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("custom_udt_tbl");
        // Then: The column with the user type should be represented and match the type previously retrieved.
        assertThat(table.getColumn("v")).hasType(custom_udt);

        // Given: Existing data in table with a UDT with custom types.
        session().execute("INSERT INTO custom_udt_tbl (k, v) VALUES (0, {regular: 5, c1: 's@hello:i@93', c2: 400})");

        // When: Data is retrieved.
        Row row = session().execute("select * from custom_udt_tbl").one();

        // Then: The resulting row's column definitions should match the table definition.
        assertThat(row.getColumnDefinitions().getType("k")).isEqualTo(cint());
        assertThat(row.getColumnDefinitions().getType("v")).isEqualTo(custom_udt);

        // And: The column values should represent what was inserted.
        UDTValue value = row.getUDTValue("v");
        assertThat(value.getInt("regular")).isEqualTo(5);
        assertThat(value.getBytes("c1")).isEqualTo(serializeForDynamicCompositeType("hello", 93));
        assertThat(value.getLong("c2")).isEqualTo(400);
    }
}
