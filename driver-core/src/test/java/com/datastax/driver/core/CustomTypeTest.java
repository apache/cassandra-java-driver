/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.custom;

/**
 * Test we "support" custom types.
 */
public class CustomTypeTest extends CCMBridge.PerClassSingleNodeCluster {

    public static final DataType CUSTOM_DYNAMIC_COMPOSITE = custom(
            "org.apache.cassandra.db.marshal.DynamicCompositeType("
                    + "s=>org.apache.cassandra.db.marshal.UTF8Type,"
                    + "i=>org.apache.cassandra.db.marshal.Int32Type)");

    public static final DataType CUSTOM_COMPOSITE = custom(
            "org.apache.cassandra.db.marshal.CompositeType("
                    + "org.apache.cassandra.db.marshal.UTF8Type,"
                    + "org.apache.cassandra.db.marshal.Int32Type)");

    @Override
    protected Collection<String> getTableDefinitions() {
        return Collections.singleton(
                "CREATE TABLE test ("
                        + "    k int,"
                        + "    c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                        + "    c2 'ReversedType(CompositeType(UTF8Type, Int32Type))'," // reversed translates to CLUSTERING ORDER BY DESC
                        + "    c3 'Int32Type'," // translates to int
                        + "    PRIMARY KEY (k, c1, c2)"
                        + ") WITH COMPACT STORAGE"
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

        TableMetadata table = cluster.getMetadata().getKeyspace(keyspace).getTable("test");

        assertThat(table.getColumn("c1")).isClusteringColumn().hasType(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(table.getColumn("c2")).isClusteringColumn().hasType(CUSTOM_COMPOSITE).hasClusteringOrder(ClusteringOrder.DESC);
        assertThat(table.getColumn("c3")).hasType(cint());

        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 's@foo:i@32', 'foo:32', 1)");
        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@42', ':42', 2)");
        session.execute("INSERT INTO test(k, c1, c2, c3) VALUES (0, 'i@12:i@3', 'foo', 3)");

        ResultSet rs = session.execute("SELECT * FROM test");

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
    @CassandraVersion(major = 2.1)
    public void should_handle_udt_with_custom_type() {
        // Given: a UDT with custom types, and a table using it.
        session.execute("CREATE TYPE custom_udt (regular int, c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)', c2 'LongType')");
        session.execute("CREATE TABLE custom_udt_tbl (k int primary key, v frozen<custom_udt>)");

        // When: Retrieving User Type via schema metadata.
        UserType custom_udt = cluster.getMetadata().getKeyspace(keyspace).getUserType("custom_udt");
        assertThat(custom_udt.getFieldType("regular")).isEqualTo(cint());
        // Then: The fields with custom types should be appropriately represented with their defined types.
        assertThat(custom_udt.getFieldType("c1")).isEqualTo(CUSTOM_DYNAMIC_COMPOSITE);
        assertThat(custom_udt.getFieldType("c2")).isEqualTo(DataType.bigint());

        // When: Retrieving Table via schema metadata.
        TableMetadata table = cluster.getMetadata().getKeyspace(keyspace).getTable("custom_udt_tbl");
        // Then: The column with the user type should be represented and match the type previously retrieved.
        assertThat(table.getColumn("v")).hasType(custom_udt);

        // Given: Existing data in table with a UDT with custom types.
        session.execute("INSERT INTO custom_udt_tbl (k, v) VALUES (0, {regular: 5, c1: 's@hello:i@93', c2: 400})");

        // When: Data is retrieved.
        Row row = session.execute("select * from custom_udt_tbl").one();

        // Then: The resulting row's column definitions should match the table definition.
        assertThat(row.getColumnDefinitions().getType("k")).isEqualTo(cint());
        assertThat(row.getColumnDefinitions().getType("v")).isEqualTo(custom_udt);

        // And: The column values should represent what was inserted.
        UDTValue value = row.getUDTValue("v");
        assertThat(value.getInt("regular")).isEqualTo(5);
        assertThat(value.getBytes("c1")).isEqualTo(serializeForDynamicCompositeType("hello", 93));
        assertThat(value.getLong("c2")).isEqualTo(400);
    }


    private ByteBuffer serializeForDynamicCompositeType(Object... params) {

        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        int size = 0;
        for (Object p : params) {
            if (p instanceof Integer) {
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + 4 + 1);
                elt.putShort((short) (0x8000 | 'i'));
                elt.putShort((short) 4);
                elt.putInt((Integer) p);
                elt.put((byte) 0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else if (p instanceof String) {
                ByteBuffer bytes = ByteBuffer.wrap(((String) p).getBytes());
                ByteBuffer elt = ByteBuffer.allocate(2 + 2 + bytes.remaining() + 1);
                elt.putShort((short) (0x8000 | 's'));
                elt.putShort((short) bytes.remaining());
                elt.put(bytes);
                elt.put((byte) 0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else {
                throw new RuntimeException();
            }
        }
        ByteBuffer res = ByteBuffer.allocate(size);
        for (ByteBuffer bb : l)
            res.put(bb);
        res.flip();
        return res;
    }

    private ByteBuffer serializeForCompositeType(Object... params) {

        List<ByteBuffer> l = new ArrayList<ByteBuffer>();
        int size = 0;
        for (Object p : params) {
            if (p instanceof Integer) {
                ByteBuffer elt = ByteBuffer.allocate(2 + 4 + 1);
                elt.putShort((short) 4);
                elt.putInt((Integer) p);
                elt.put((byte) 0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else if (p instanceof String) {
                ByteBuffer bytes = ByteBuffer.wrap(((String) p).getBytes());
                ByteBuffer elt = ByteBuffer.allocate(2 + bytes.remaining() + 1);
                elt.putShort((short) bytes.remaining());
                elt.put(bytes);
                elt.put((byte) 0);
                elt.flip();
                size += elt.remaining();
                l.add(elt);
            } else {
                throw new RuntimeException();
            }
        }
        ByteBuffer res = ByteBuffer.allocate(size);
        for (ByteBuffer bb : l)
            res.put(bb);
        res.flip();
        return res;
    }

}
