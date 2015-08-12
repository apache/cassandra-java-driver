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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import com.datastax.driver.core.ColumnMetadata.*;
import com.datastax.driver.core.Token.M3PToken;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ColumnMetadata.*;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.text;

@CassandraVersion(major = 1.2)
public class IndexMetadataTest extends CCMBridge.PerClassSingleNodeCluster {

    /**
     * Column definitions for schema_columns table.
     */
    private static final ColumnDefinitions defs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
        definition(COLUMN_NAME, text()),
        definition(COMPONENT_INDEX, cint()),
        definition(KIND, text()),
        definition(INDEX_NAME, text()),
        definition(INDEX_TYPE, text()),
        definition(VALIDATOR, text()),
        definition(INDEX_OPTIONS, text())
    });

    static {
        defs.setCodecRegistry(new CodecRegistry());
    }

    @Override
    protected Collection<String> getTableDefinitions() {
        String createTable = "CREATE TABLE indexing ("
            + "id int primary key,"
            + "map_values map<text, int>,"
            + "map_keys map<text, int>,"
            + "map_entries map<text, int>,"
            + "text_column text"
            +
            // Frozen collections was introduced only in C* 2.1.3
            (cassandraVersion.compareTo(VersionNumber.parse("2.1.3")) >= 0
                ?
                ", map_full frozen<map<text, int>>,"
                    + "set_full frozen<set<text>>,"
                    + "list_full frozen<list<text>>);"
                :
                ")");
        return ImmutableList.of(createTable);
    }

    @Test(groups = "short")
    public void should_not_flag_text_column_index_type() {
        String createValuesIndex = String.format("CREATE INDEX text_column_index ON %s.indexing (text_column);", keyspace);
        session.execute(createValuesIndex);
        ColumnMetadata column = getColumn("text_column");
        IndexMetadata index = getIndex("text_column_index");
        assertThat(index).hasName("text_column_index")
            .isNotKeys()
            .isNotFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("text_column"))
            .asCqlQuery(createValuesIndex);
        assertThat(index).isSameAs(column.getIndex("text_column_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("text_column"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1)
    public void should_not_flag_map_index_type() {
        String createValuesIndex = String.format("CREATE INDEX map_values_index ON %s.indexing (map_values);", keyspace);
        session.execute(createValuesIndex);
        ColumnMetadata column = getColumn("map_values");
        IndexMetadata index = getIndex("map_values_index");
        assertThat(index).hasName("map_values_index")
            .isNotKeys()
            .isNotFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("map_values"))
            .asCqlQuery(createValuesIndex);
        assertThat(index).isSameAs(column.getIndex("map_values_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("map_values"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1)
    public void should_flag_map_index_type_as_keys() {
        String createKeysIndex = String.format("CREATE INDEX map_keys_index ON %s.indexing (KEYS(map_keys));", keyspace);
        session.execute(createKeysIndex);
        ColumnMetadata column = getColumn("map_keys");
        IndexMetadata index = getIndex("map_keys_index");
        assertThat(index).hasName("map_keys_index")
            .isKeys()
            .isNotFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("map_keys"))
            .asCqlQuery(createKeysIndex);
        assertThat(index).isSameAs(column.getIndex("map_keys_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("map_keys"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1, minor = 3)
    public void should_flag_map_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX map_full_index ON %s.indexing (FULL(map_full));", keyspace);
        session.execute(createFullIndex);
        ColumnMetadata column = getColumn("map_full");
        IndexMetadata index = getIndex("map_full_index");
        assertThat(index).hasName("map_full_index")
            .isNotKeys()
            .isFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("map_full"))
            .asCqlQuery(createFullIndex);
        assertThat(index).isSameAs(column.getIndex("map_full_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("map_full"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1, minor = 3)
    public void should_flag_set_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX set_full_index ON %s.indexing (FULL(set_full));", keyspace);
        session.execute(createFullIndex);
        ColumnMetadata column = getColumn("set_full");
        IndexMetadata index = getIndex("set_full_index");
        assertThat(index).hasName("set_full_index")
            .isNotKeys()
            .isFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("set_full"))
            .asCqlQuery(createFullIndex);
        assertThat(index).isSameAs(column.getIndex("set_full_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("set_full"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.1, minor = 3)
    public void should_flag_list_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX list_full_index ON %s.indexing (FULL(list_full));", keyspace);
        session.execute(createFullIndex);
        ColumnMetadata column = getColumn("list_full");
        IndexMetadata index = getIndex("list_full_index");
        assertThat(index).hasName("list_full_index")
            .isNotKeys()
            .isFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("list_full"))
            .asCqlQuery(createFullIndex);
        assertThat(index).isSameAs(column.getIndex("list_full_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("list_full"));
    }

    @Test(groups = "short")
    @CassandraVersion(major = 2.2)
    public void should_flag_map_index_type_as_entries() {
        String createEntriesIndex = String.format("CREATE INDEX map_entries_index ON %s.indexing (ENTRIES(map_entries));", keyspace);
        session.execute(createEntriesIndex);
        ColumnMetadata column = getColumn("map_entries");
        IndexMetadata index = getIndex("map_entries_index");
        assertThat(index).hasName("map_entries_index")
            .isNotKeys()
            .isNotFull()
            .isEntries()
            .isNotCustomIndex()
            .hasSingleColumn(getColumn("map_entries"))
            .asCqlQuery(createEntriesIndex);
        assertThat(index).isSameAs(column.getIndex("map_entries_index"));
        assertThat(column).hasSingleIndex(index);
        assertThat(column).isSameAs(index.getColumn("map_entries"));
    }

    @Test(
        groups = "short",
        description = "This test case builds a ColumnMetadata object programmatically to test custom indices,"
            + "otherwise, it would require deploying an actual custom index class into the C* test cluster")
    public void should_parse_custom_index_options() {
        TableMetadata table = getTable("indexing");
        List<ByteBuffer> data = ImmutableList.of(
            wrap("text_column"), // column name
            wrap(0), // component index
            wrap("regular"), // kind
            wrap("custom_index"), // index name
            wrap("CUSTOM"), // index type
            wrap("org.apache.cassandra.db.marshal.UTF8Type"), // validator
            wrap("{\"foo\" : \"bar\", \"class_name\" : \"dummy.DummyIndex\"}") // index options
        );
        Row row = ArrayBackedRow.fromData(defs, M3PToken.FACTORY, cluster.getConfiguration().getProtocolOptions().getProtocolVersion(), data);
        Raw raw = Raw.fromRow(row, VersionNumber.parse("2.1"),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
        ColumnMetadata column = ColumnMetadata.fromRaw(table, raw);
        IndexMetadata index = IndexMetadata.fromLegacy(column, raw);
        assertThat(index).hasName("custom_index")
            .isNotKeys()
            .isNotFull()
            .isNotEntries()
            .isCustomIndex()
            .hasSingleColumn(column)
            .hasOption("foo", "bar")
            .asCqlQuery(String.format("CREATE CUSTOM INDEX custom_index ON %s.indexing (text_column) "
                + "USING 'dummy.DummyIndex' WITH OPTIONS = {'foo' : 'bar', 'class_name' : 'dummy.DummyIndex'};", keyspace));
    }

    /**
     * Validates a special case where a 'KEYS' index was created using thrift.  In this particular case the index lacks
     * index_options, however the index_options value is a 'null' string rather then a null value.
     *
     * @test_category metadata
     * @expected_result Index properly parsed and is present.
     * @jira_ticket JAVA-834
     * @since 2.0.11, 2.1.7
     */
    @Test(groups = "short")
    public void should_parse_with_null_string_index_options() {
        TableMetadata table = getTable("indexing");
        List<ByteBuffer> data = ImmutableList.of(
            wrap("b@706172656e745f70617468"), // column name 'parent_path'
            ByteBuffer.allocate(0), // component index (null)
            wrap("regular"), // kind
            wrap("cfs_archive_parent_path"), // index name
            wrap("KEYS"), // index type
            wrap("org.apache.cassandra.db.marshal.BytesType"), // validator
            wrap("null") // index options
        );
        Row row = ArrayBackedRow.fromData(defs, M3PToken.FACTORY, cluster.getConfiguration().getProtocolOptions().getProtocolVersion(), data);
        Raw raw = Raw.fromRow(row, VersionNumber.parse("2.1"),
            cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
            cluster.getConfiguration().getCodecRegistry());
        ColumnMetadata column = ColumnMetadata.fromRaw(table, raw);
        IndexMetadata index = IndexMetadata.fromLegacy(column, raw);
        assertThat(index).hasName("cfs_archive_parent_path")
            .isNotKeys() // While the index type is KEYS, since it lacks index_options it does not considered.
            .isNotFull()
            .isNotEntries()
            .isNotCustomIndex()
            .hasSingleColumn(column)
            .asCqlQuery(String.format("CREATE INDEX cfs_archive_parent_path ON %s.indexing (\"b@706172656e745f70617468\");", keyspace));
    }

    private static ColumnDefinitions.Definition definition(String name, DataType type) {
        return new ColumnDefinitions.Definition("ks", "table", name, type);
    }

    private static ByteBuffer wrap(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

    private static ByteBuffer wrap(int number) {
        return ByteBuffer.wrap(Ints.toByteArray(number));
    }

    private ColumnMetadata getColumn(String name) {
        return getTable("indexing").getColumn(name);
    }

    private IndexMetadata getIndex(String name) {
        return getTable("indexing").getIndex(name);
    }

    private TableMetadata getTable(String name) {
        return cluster.getMetadata().getKeyspace(keyspace).getTable(name);
    }

}
