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

import com.datastax.driver.core.ColumnMetadata.*;
import com.datastax.driver.core.Token.M3PToken;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ColumnMetadata.*;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.IndexMetadata.Kind.*;

@CassandraVersion("1.2.0")
public class IndexMetadataTest extends CCMTestsSupport {

    /**
     * Column definitions for schema_columns table (legacy pre-3.0 layout).
     */
    private static final ColumnDefinitions legacyColumnDefs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
            definition(COLUMN_NAME, text()),
            definition(COMPONENT_INDEX, cint()),
            definition(KIND_V2, text()),
            definition(INDEX_NAME, text()),
            definition(INDEX_TYPE, text()),
            definition(VALIDATOR, text()),
            definition(INDEX_OPTIONS, text())
    }, CodecRegistry.DEFAULT_INSTANCE);

    /**
     * Column definitions for indexes table (post-3.0 layout).
     */
    private static final ColumnDefinitions indexColumnDefs = new ColumnDefinitions(new ColumnDefinitions.Definition[]{
            definition(IndexMetadata.NAME, text()),
            definition(IndexMetadata.KIND, text()),
            definition(IndexMetadata.OPTIONS, map(text(), text()))
    }, CodecRegistry.DEFAULT_INSTANCE);

    private static final TypeCodec<Map<String, String>> MAP_CODEC = TypeCodec.map(TypeCodec.varchar(), TypeCodec.varchar());

    private ProtocolVersion protocolVersion;

    @Override
    public void onTestContextInitialized() {
        protocolVersion = cluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        String createTable = "CREATE TABLE indexing ("
                + "id int,"
                + "id2 int,"
                + "map_values map<text, int>,"
                + "map_keys map<text, int>,"
                + "map_entries map<text, int>,"
                + "map_all map<text, int>,"
                + "text_column text, "
                + "\"MixedCaseColumn\" list<text>,"
                +
                // Frozen collections was introduced only in C* 2.1.3
                (ccm().getCassandraVersion().compareTo(VersionNumber.parse("2.1.3")) >= 0
                        ?
                        ", map_full frozen<map<text, int>>,"
                                + "set_full frozen<set<text>>,"
                                + "list_full frozen<list<text>>,"
                        :
                        "")
                + "PRIMARY KEY (id, id2));";
        execute(createTable);
    }

    @Test(groups = "short")
    public void should_create_metadata_for_simple_index() {
        String createValuesIndex = String.format("CREATE INDEX text_column_index ON %s.indexing (text_column);", keyspace);
        session().execute(createValuesIndex);
        ColumnMetadata column = getColumn("text_column");
        IndexMetadata index = getIndex("text_column_index");
        assertThat(index)
                .hasName("text_column_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("text_column")
                .hasKind(COMPOSITES)
                .asCqlQuery(createValuesIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion(value = "2.1", description = "index names with quoted identifiers and collection indexes not supported until 2.1")
    public void should_create_metadata_for_values_index_on_mixed_case_column() {
        // 3.0 assumes the 'values' keyword if index on a collection
        String createValuesIndex = ccm().getCassandraVersion().getMajor() > 2 ?
                String.format("CREATE INDEX \"MixedCaseIndex\" ON %s.indexing (values(\"MixedCaseColumn\"));", keyspace) :
                String.format("CREATE INDEX \"MixedCaseIndex\" ON %s.indexing (\"MixedCaseColumn\");", keyspace);
        session().execute(createValuesIndex);
        ColumnMetadata column = getColumn("\"MixedCaseColumn\"");
        IndexMetadata index = getIndex("\"MixedCaseIndex\"");
        assertThat(index)
                .hasName("MixedCaseIndex")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget(ccm().getCassandraVersion().getMajor() > 2 ? "values(\"MixedCaseColumn\")" : "\"MixedCaseColumn\"")
                .hasKind(COMPOSITES)
                .asCqlQuery(createValuesIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_create_metadata_for_index_on_map_values() {
        // 3.0 assumes the 'values' keyword if index on a collection
        String createValuesIndex = ccm().getCassandraVersion().getMajor() > 2 ?
                String.format("CREATE INDEX map_values_index ON %s.indexing (values(map_values));", keyspace) :
                String.format("CREATE INDEX map_values_index ON %s.indexing (map_values);", keyspace);
        session().execute(createValuesIndex);
        ColumnMetadata column = getColumn("map_values");
        IndexMetadata index = getIndex("map_values_index");
        assertThat(index)
                .hasName("map_values_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget(ccm().getCassandraVersion().getMajor() > 2 ? "values(map_values)" : "map_values")
                .hasKind(COMPOSITES)
                .asCqlQuery(createValuesIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_create_metadata_for_index_on_map_keys() {
        String createKeysIndex = String.format("CREATE INDEX map_keys_index ON %s.indexing (keys(map_keys));", keyspace);
        session().execute(createKeysIndex);
        ColumnMetadata column = getColumn("map_keys");
        IndexMetadata index = getIndex("map_keys_index");
        assertThat(index)
                .hasName("map_keys_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("keys(map_keys)")
                .hasKind(COMPOSITES)
                .asCqlQuery(createKeysIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.3")
    public void should_create_metadata_for_full_index_on_map() {
        String createFullIndex = String.format("CREATE INDEX map_full_index ON %s.indexing (full(map_full));", keyspace);
        session().execute(createFullIndex);
        ColumnMetadata column = getColumn("map_full");
        IndexMetadata index = getIndex("map_full_index");
        assertThat(index)
                .hasName("map_full_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("full(map_full)")
                .hasKind(COMPOSITES)
                .asCqlQuery(createFullIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.3")
    public void should_create_metadata_for_full_index_on_set() {
        String createFullIndex = String.format("CREATE INDEX set_full_index ON %s.indexing (full(set_full));", keyspace);
        session().execute(createFullIndex);
        ColumnMetadata column = getColumn("set_full");
        IndexMetadata index = getIndex("set_full_index");
        assertThat(index)
                .hasName("set_full_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("full(set_full)")
                .hasKind(COMPOSITES)
                .asCqlQuery(createFullIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.3")
    public void should_create_metadata_for_full_index_on_list() {
        String createFullIndex = String.format("CREATE INDEX list_full_index ON %s.indexing (full(list_full));", keyspace);
        session().execute(createFullIndex);
        ColumnMetadata column = getColumn("list_full");
        IndexMetadata index = getIndex("list_full_index");
        assertThat(index)
                .hasName("list_full_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("full(list_full)")
                .hasKind(COMPOSITES)
                .asCqlQuery(createFullIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("2.2.0")
    public void should_create_metadata_for_index_on_map_entries() {
        String createEntriesIndex = String.format("CREATE INDEX map_entries_index ON %s.indexing (entries(map_entries));", keyspace);
        session().execute(createEntriesIndex);
        ColumnMetadata column = getColumn("map_entries");
        IndexMetadata index = getIndex("map_entries_index");
        assertThat(index)
                .hasName("map_entries_index")
                .hasParent((TableMetadata) column.getParent())
                .isNotCustomIndex()
                .hasTarget("entries(map_entries)")
                .hasKind(COMPOSITES)
                .asCqlQuery(createEntriesIndex);
        assertThat((TableMetadata) column.getParent()).hasIndex(index);
    }

    @Test(groups = "short")
    @CassandraVersion("3.0")
    public void should_allow_multiple_indexes_on_map_column() {
        String createEntriesIndex = String.format("CREATE INDEX map_all_entries_index ON %s.indexing (entries(map_all));", keyspace);
        session().execute(createEntriesIndex);
        String createKeysIndex = String.format("CREATE INDEX map_all_keys_index ON %s.indexing (keys(map_all));", keyspace);
        session().execute(createKeysIndex);
        String createValuesIndex = String.format("CREATE INDEX map_all_values_index ON %s.indexing (values(map_all));", keyspace);
        session().execute(createValuesIndex);

        ColumnMetadata column = getColumn("map_all");
        TableMetadata table = (TableMetadata) column.getParent();

        assertThat(getIndex("map_all_entries_index"))
                .hasParent(table)
                .asCqlQuery(createEntriesIndex);

        assertThat(getIndex("map_all_keys_index"))
                .hasParent(table)
                .asCqlQuery(createKeysIndex);

        assertThat(getIndex("map_all_values_index"))
                .hasParent(table)
                .asCqlQuery(createValuesIndex);
    }

    @Test(
            groups = "short",
            description = "This test case builds a ColumnMetadata object programmatically to test custom indices with pre-3.0 layout,"
                    + "otherwise, it would require deploying an actual custom index class into the C* test cluster")
    public void should_parse_legacy_custom_index_options() {
        TableMetadata table = getTable("indexing");
        List<ByteBuffer> columnData = ImmutableList.of(
                wrap("text_column"), // column name
                wrap(0), // component index
                wrap("regular"), // column kind
                wrap("custom_index"), // index name
                wrap("CUSTOM"), // index type
                wrap("org.apache.cassandra.db.marshal.UTF8Type"), // validator
                wrap("{\"foo\" : \"bar\", \"class_name\" : \"dummy.DummyIndex\"}") // index options
        );
        Row columnRow = ArrayBackedRow.fromData(legacyColumnDefs, M3PToken.FACTORY, protocolVersion, columnData);
        Raw columnRaw = Raw.fromRow(columnRow, VersionNumber.parse("2.1"));
        ColumnMetadata column = ColumnMetadata.fromRaw(table, columnRaw, DataType.varchar());
        IndexMetadata index = IndexMetadata.fromLegacy(column, columnRaw);
        assertThat(index)
                .isNotNull()
                .hasName("custom_index")
                .isCustomIndex()
                .hasOption("foo", "bar")
                .hasKind(CUSTOM)
                .asCqlQuery(String.format("CREATE CUSTOM INDEX custom_index ON %s.indexing (text_column) "
                        + "USING 'dummy.DummyIndex' WITH OPTIONS = {'foo' : 'bar'};", keyspace));
    }

    @Test(
            groups = "short",
            description = "This test case builds a ColumnMetadata object programmatically to test custom indices with post-3.0 layout,"
                    + "otherwise, it would require deploying an actual custom index class into the C* test cluster")
    public void should_parse_custom_index_options() {
        TableMetadata table = getTable("indexing");
        List<ByteBuffer> indexData = ImmutableList.of(
                wrap("custom_index"), // index name
                wrap("CUSTOM"), // kind
                MAP_CODEC.serialize(ImmutableMap.of(
                        "foo", "bar",
                        IndexMetadata.CUSTOM_INDEX_OPTION_NAME, "dummy.DummyIndex",
                        IndexMetadata.TARGET_OPTION_NAME, "a, b, keys(c)"
                ), protocolVersion) // options
        );
        Row indexRow = ArrayBackedRow.fromData(indexColumnDefs, M3PToken.FACTORY, protocolVersion, indexData);
        IndexMetadata index = IndexMetadata.fromRow(table, indexRow);
        assertThat(index)
                .isNotNull()
                .hasName("custom_index")
                .isCustomIndex()
                .hasOption("foo", "bar")
                .hasTarget("a, b, keys(c)")
                .hasKind(CUSTOM)
                .asCqlQuery(String.format("CREATE CUSTOM INDEX custom_index ON %s.indexing (a, b, keys(c)) "
                        + "USING 'dummy.DummyIndex' WITH OPTIONS = {'foo' : 'bar'};", keyspace));
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
        Row row = ArrayBackedRow.fromData(legacyColumnDefs, M3PToken.FACTORY, cluster().getConfiguration().getProtocolOptions().getProtocolVersion(), data);
        Raw raw = Raw.fromRow(row, VersionNumber.parse("2.1"));
        ColumnMetadata column = ColumnMetadata.fromRaw(table, raw, DataType.blob());
        IndexMetadata index = IndexMetadata.fromLegacy(column, raw);
        assertThat(index)
                .isNotNull()
                .hasName("cfs_archive_parent_path")
                .isNotCustomIndex()
                .hasTarget("\"b@706172656e745f70617468\"")
                .hasKind(KEYS)
                .asCqlQuery(String.format("CREATE INDEX cfs_archive_parent_path ON %s.indexing (\"b@706172656e745f70617468\");", keyspace));
        assertThat(index.getOption(IndexMetadata.INDEX_KEYS_OPTION_NAME)).isNull(); // While the index type is KEYS, since it lacks index_options it does not get considered.
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
        return getColumn(name, true);
    }

    private ColumnMetadata getColumn(String name, boolean fromTable) {
        AbstractTableMetadata target = fromTable ? getTable("indexing") : getMaterializedView("mv1");
        return target.getColumn(name);
    }

    private IndexMetadata getIndex(String name) {
        return getTable("indexing").getIndex(name);
    }

    private TableMetadata getTable(String name) {
        return cluster().getMetadata().getKeyspace(keyspace).getTable(name);
    }

    private MaterializedViewMetadata getMaterializedView(String name) {
        return cluster().getMetadata().getKeyspace(keyspace).getMaterializedView(name);
    }
}
