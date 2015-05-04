package com.datastax.driver.core;

import static com.datastax.driver.core.ColumnMetadata.COLUMN_NAME;
import static com.datastax.driver.core.ColumnMetadata.COMPONENT_INDEX;
import static com.datastax.driver.core.ColumnMetadata.INDEX_NAME;
import static com.datastax.driver.core.ColumnMetadata.INDEX_OPTIONS;
import static com.datastax.driver.core.ColumnMetadata.INDEX_TYPE;
import static com.datastax.driver.core.ColumnMetadata.KIND;
import static com.datastax.driver.core.ColumnMetadata.VALIDATOR;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.text;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.datastax.driver.core.ColumnMetadata.IndexMetadata;
import com.datastax.driver.core.ColumnMetadata.Raw;
import com.datastax.driver.core.Token.M3PToken;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

@CassandraVersion(
    major = 2.1)
public class IndexMetadataTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        String createTable = "CREATE TABLE indexing ("
            + "id int primary key,"
            + "map_values map<text, int>,"
            + "map_keys map<text, int>,"
            + "map_entries map<text, int>,"
            + "map_full frozen<map<text, int>>,"
            + "set_full frozen<set<text>>,"
            + "list_full frozen<list<text>>,"
            + "custom text)";
        return ImmutableList.of(createTable);
    }

    @Test(
        groups = "short")
    public void should_not_flag_map_index_type() {
        String createValuesIndex = String.format("CREATE INDEX map_values_index ON %s.indexing (map_values);", keyspace);
        session.execute(createValuesIndex);
        IndexMetadata index = getIndexForColumn("map_values");
        Assert.assertEquals(index.getName(), "map_values_index");
        Assert.assertFalse(index.isKeys());
        Assert.assertFalse(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createValuesIndex);
    }

    @Test(
        groups = "short")
    public void should_flag_map_index_type_as_keys() {
        String createKeysIndex = String.format("CREATE INDEX map_keys_index ON %s.indexing (KEYS(map_keys));", keyspace);
        session.execute(createKeysIndex);
        IndexMetadata index = getIndexForColumn("map_keys");
        Assert.assertEquals(index.getName(), "map_keys_index");
        Assert.assertTrue(index.isKeys());
        Assert.assertFalse(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createKeysIndex);
    }

    @Test(
        groups = "short")
    @CassandraVersion(
        major = 2.1,
        minor = 3)
    public void should_flag_map_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX map_full_index ON %s.indexing (FULL(map_full));", keyspace);
        session.execute(createFullIndex);
        IndexMetadata index = getIndexForColumn("map_full");
        Assert.assertEquals(index.getName(), "map_full_index");
        Assert.assertFalse(index.isKeys());
        Assert.assertTrue(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createFullIndex);
    }

    @Test(
        groups = "short")
    @CassandraVersion(
        major = 2.1,
        minor = 3)
    public void should_flag_set_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX set_full_index ON %s.indexing (FULL(set_full));", keyspace);
        session.execute(createFullIndex);
        IndexMetadata index = getIndexForColumn("set_full");
        Assert.assertEquals(index.getName(), "set_full_index");
        Assert.assertFalse(index.isKeys());
        Assert.assertTrue(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createFullIndex);
    }

    @Test(
        groups = "short")
    @CassandraVersion(
        major = 2.1,
        minor = 3)
    public void should_flag_list_index_type_as_full() {
        String createFullIndex = String.format("CREATE INDEX list_full_index ON %s.indexing (FULL(list_full));", keyspace);
        session.execute(createFullIndex);
        IndexMetadata index = getIndexForColumn("list_full");
        Assert.assertEquals(index.getName(), "list_full_index");
        Assert.assertFalse(index.isKeys());
        Assert.assertTrue(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createFullIndex);
    }

    @Test(
        groups = "short")
    @CassandraVersion(
        major = 3.0)
    public void should_flag_map_index_type_as_entries() {
        String createEntriesIndex = String.format("CREATE INDEX map_entries_index ON %s.indexing (ENTRIES(map_entries));", keyspace);
        session.execute(createEntriesIndex);
        IndexMetadata index = getIndexForColumn("map_entries");
        Assert.assertEquals(index.getName(), "map_entries_index");
        Assert.assertFalse(index.isKeys());
        Assert.assertFalse(index.isFull());
        Assert.assertTrue(index.isEntries());
        Assert.assertFalse(index.isCustomIndex());
        Assert.assertEquals(index.asCQLQuery(), createEntriesIndex);
    }

    @Test(
        groups = "short",
        description = "This test case builds a ColumnMetadata object programatically to test custom indices,"
            + "otherwise, it would require deploying an actual custom index class into the C* test cluster")
    public void should_parse_custom_index_options() {
        TableMetadata table = getTable("indexing");
        ColumnDefinitions defs = new ColumnDefinitions(new ColumnDefinitions.Definition[] {
                definition(COLUMN_NAME, text()),
                definition(COMPONENT_INDEX, cint()),
                definition(KIND, text()),
                definition(INDEX_NAME, text()),
                definition(INDEX_TYPE, text()),
                definition(VALIDATOR, text()),
                definition(INDEX_OPTIONS, text())
        });
        List<ByteBuffer> data = ImmutableList.of(
            wrap("custom"), // column name
            wrap(0), // component index
            wrap("regular"), // kind
            wrap("custom_index"), // index name
            wrap("CUSTOM"), // index type
            wrap("org.apache.cassandra.db.marshal.UTF8Type"), // validator
            wrap("{\"foo\" : \"bar\", \"class_name\" : \"dummy.DummyIndex\"}") // index options
            );
        Row row = ArrayBackedRow.fromData(defs, M3PToken.FACTORY, ProtocolVersion.V3, data);
        ColumnMetadata column = ColumnMetadata.fromRaw(table, Raw.fromRow(row, VersionNumber.parse("2.1")));
        IndexMetadata index = column.getIndex();
        Assert.assertFalse(index.isKeys());
        Assert.assertFalse(index.isFull());
        Assert.assertFalse(index.isEntries());
        Assert.assertTrue(index.isCustomIndex());
        Assert.assertEquals(index.getOption("foo"), "bar");
        Assert.assertEquals(index.asCQLQuery(), "CREATE CUSTOM INDEX custom_index ON ks_1.indexing (custom) "
            + "USING 'dummy.DummyIndex' WITH OPTIONS = {'foo' : 'bar', 'class_name' : 'dummy.DummyIndex'};");
    }

    private ColumnDefinitions.Definition definition(String name, DataType type) {
        return new ColumnDefinitions.Definition("ks", "table", name, type);
    }

    private ByteBuffer wrap(String value) {
        return ByteBuffer.wrap(value.getBytes());
    }

    private ByteBuffer wrap(int number) {
        return ByteBuffer.wrap(Ints.toByteArray(number));
    }

    private IndexMetadata getIndexForColumn(String columnName) {
        return getColumn(columnName).getIndex();
    }

    private ColumnMetadata getColumn(String name) {
        return getTable("indexing").getColumn(name);
    }

    private TableMetadata getTable(String name) {
        return cluster.getMetadata().getKeyspace(keyspace).getTable(name);
    }

}
