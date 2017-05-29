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
package com.datastax.driver.core.schemabuilder;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions.TimeStampResolution;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import java.util.Iterator;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class SchemaBuilderIT extends CCMTestsSupport {

    @Test(groups = "short")
    @CassandraVersion("2.1.2")
    public void should_modify_table_metadata() {
        // Create a table
        session().execute(SchemaBuilder.createTable("ks", "TableMetadata")
                        .addPartitionKey("a", DataType.cint())
                        .addPartitionKey("b", DataType.cint())
                        .addClusteringColumn("c", DataType.cint())
                        .addClusteringColumn("d", DataType.cint())
                        .withOptions()
                        .compactStorage()
        );

        // Modify the table metadata
        session().execute(SchemaBuilder.alterTable("TableMetadata")
                .withOptions()
                .defaultTimeToLive(1337)
                .bloomFilterFPChance(0.42)
                .caching(SchemaBuilder.KeyCaching.ALL, rows(1))
                .gcGraceSeconds(1234567890)
                .minIndexInterval(6)
                .indexInterval(7)
                .maxIndexInterval(8)
                .comment("Useful comment")
                .readRepairChance(0.123456)
                .speculativeRetry(percentile(50))
                .dcLocalReadRepairChance(0.84)
                .memtableFlushPeriodInMillis(1234567890)
                .compactionOptions(dateTieredStrategy()
                        .baseTimeSeconds(1)
                        .minThreshold(2)
                        .maxThreshold(3)
                        .maxSSTableAgeDays(4)
                        .timestampResolution(TimeStampResolution.MILLISECONDS))
                .compressionOptions(snappy()));

        // Retrieve the metadata from Cassandra
        ResultSet rows = session().execute("SELECT "
                + "bloom_filter_fp_chance, "
                + "caching, "
                + "cf_id, "
                + "column_aliases, "
                + "comment, "
                + "compaction_strategy_class, "
                + "compaction_strategy_options, "
                + "comparator, "
                + "compression_parameters, "
                + "default_time_to_live, "
                + "default_validator, "
                + "dropped_columns, "
                + "gc_grace_seconds, "
                + "index_interval, "
                + "is_dense, "
                + "key_aliases, "
                + "key_validator, "
                + "local_read_repair_chance, "
                + "max_compaction_threshold, "
                + "max_index_interval, "
                + "memtable_flush_period_in_ms, "
                + "min_compaction_threshold, "
                + "min_index_interval, "
                + "read_repair_chance, "
                + "speculative_retry, "
                + "subcomparator, "
                + "type, "
                + "value_alias "
                + "FROM system.schema_columnfamilies "
                + "WHERE keyspace_name='ks' AND columnfamily_name='tablemetadata'");
        for (Row row : rows) {
            // There should be only one row
            // Verify that every property we modified is correctly set
            assertThat(row.getDouble("bloom_filter_fp_chance")).isEqualTo(0.42);
            assertThat(row.getString("caching")).isEqualTo("{\"keys\":\"ALL\", \"rows_per_partition\":\"1\"}");
            assertThat(row.getUUID("cf_id")).isNotNull();
            assertThat(row.getString("column_aliases")).isEqualTo("[\"c\",\"d\"]");
            assertThat(row.getString("comment")).isEqualTo("Useful comment");
            assertThat(row.getString("compaction_strategy_class"))
                    .isEqualTo("org.apache.cassandra.db.compaction.DateTieredCompactionStrategy");
            assertThat(row.getString("compaction_strategy_options")).isEqualTo(
                    "{\"base_time_seconds\":\"1\",\"timestamp_resolution\":\"MILLISECONDS\",\"max_sstable_age_days\":\"4\",\"min_threshold\":\"2\",\"max_threshold\":\"3\"}");
            assertThat(row.getString("compression_parameters"))
                    .isEqualTo("{\"sstable_compression\":\"org.apache.cassandra.io.compress.SnappyCompressor\"}");
            assertThat(row.getInt("default_time_to_live")).isEqualTo(1337);
            assertThat(row.getInt("gc_grace_seconds")).isEqualTo(1234567890);
            assertThat(row.getInt("min_index_interval")).isEqualTo(6);
            assertThat(row.getInt("max_index_interval")).isEqualTo(8);
            assertThat(row.getString("key_aliases")).isEqualTo("[\"a\",\"b\"]");
            assertThat(row.getDouble("local_read_repair_chance")).isEqualTo(0.84);
            assertThat(row.getInt("max_compaction_threshold")).isEqualTo(3);
            assertThat(row.getInt("memtable_flush_period_in_ms")).isEqualTo(1234567890);
            assertThat(row.getInt("min_compaction_threshold")).isEqualTo(2);
            assertThat(row.getDouble("read_repair_chance")).isEqualTo(0.123456);
            assertThat(row.getString("speculative_retry")).isEqualTo("50.0PERCENTILE");
        }
    }

    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_create_a_table_and_a_udt() {
        // Create a UDT and a table
        session().execute(SchemaBuilder.createType("MyUDT")
                        .ifNotExists()
                        .addColumn("x", DataType.cint())
        );
        UDTType myUDT = UDTType.frozen("MyUDT");
        session().execute(SchemaBuilder.createTable("ks", "CreateTable")
                        .ifNotExists()
                        .addPartitionKey("a", DataType.cint())
                        .addUDTPartitionKey("b", myUDT)
                        .addClusteringColumn("c", DataType.ascii())
                        .addUDTClusteringColumn("d", myUDT)
                        .addUDTColumn("e", myUDT)
                        .addStaticColumn("f", DataType.bigint())
                        .addUDTStaticColumn("g", myUDT)
                        .addUDTListColumn("h", myUDT)
                        .addUDTMapColumn("i", DataType.cboolean(), myUDT)
                        .addUDTMapColumn("j", myUDT, DataType.cboolean())
                        .addUDTSetColumn("k", myUDT)
        );

        // Check columns a to k
        ResultSet rows = session().execute(
                "SELECT column_name, type, validator "
                        + "FROM system.schema_columns "
                        + "WHERE keyspace_name='ks' AND columnfamily_name='createtable'");
        Iterator<Row> iterator = rows.iterator();
        verifyNextColumnDefinition(iterator, "a", "partition_key", "org.apache.cassandra.db.marshal.Int32Type");
        verifyNextColumnDefinition(iterator, "b", "partition_key", "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "c", "clustering_key", "org.apache.cassandra.db.marshal.AsciiType");
        verifyNextColumnDefinition(iterator, "d", "clustering_key", "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "e", "regular", "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "f", "static", "org.apache.cassandra.db.marshal.LongType");
        verifyNextColumnDefinition(iterator, "g", "static", "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "h", "regular", "org.apache.cassandra.db.marshal.ListType"
                , "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "i", "regular", "org.apache.cassandra.db.marshal.MapType"
                , "org.apache.cassandra.db.marshal.BooleanType", "org.apache.cassandra.db.marshal.UserType");
        verifyNextColumnDefinition(iterator, "j", "regular", "org.apache.cassandra.db.marshal.MapType"
                , "org.apache.cassandra.db.marshal.UserType", "org.apache.cassandra.db.marshal.BooleanType");
        verifyNextColumnDefinition(iterator, "k", "regular", "org.apache.cassandra.db.marshal.SetType"
                , "org.apache.cassandra.db.marshal.UserType");
    }

    @Test(groups = "short")
    public void should_add_and_drop_a_column() {
        // Create a table, add a column to it with an alter table statement and delete that column
        session().execute(SchemaBuilder.createTable("ks", "DropColumn")
                        .ifNotExists()
                        .addPartitionKey("a", DataType.cint())
        );

        // Add and then drop a column
        session().execute(SchemaBuilder.alterTable("ks", "DropColumn")
                        .addColumn("b")
                        .type(DataType.cint())
        );
        session().execute(SchemaBuilder.alterTable("ks", "DropColumn")
                        .dropColumn("b")
        );

        // Check that only column a exist
        ResultSet rows = session().execute(
                "SELECT column_name, type, validator "
                        + "FROM system.schema_columns "
                        + "WHERE keyspace_name='ks' AND columnfamily_name='dropcolumn'");
        Iterator<Row> iterator = rows.iterator();
        verifyNextColumnDefinition(iterator, "a", "partition_key", "org.apache.cassandra.db.marshal.Int32Type");
        assertThat(iterator.hasNext()).isFalse();
    }

    private void verifyNextColumnDefinition(Iterator<Row> rowIterator, String columnName, String type,
                                            String... validatorFragments) {
        Row rowA = rowIterator.next();
        assertThat(rowA.getString("column_name")).isEqualTo(columnName);
        assertThat(rowA.getString("type")).isEqualTo(type);
        for (String validatorFragment : validatorFragments) {
            assertThat(rowA.getString("validator")).contains(validatorFragment);
        }
    }

    @Test(groups = "short")
    public void should_drop_a_table() {
        // Create a table
        session().execute(SchemaBuilder.createTable("ks", "DropTable")
                        .addPartitionKey("a", DataType.cint())
        );

        // Drop the table
        session().execute(SchemaBuilder.dropTable("ks", "DropTable"));
        session().execute(SchemaBuilder.dropTable("DropTable").ifExists());

        ResultSet rows = session().execute(
                "SELECT columnfamily_name "
                        + "FROM system.schema_columnfamilies "
                        + "WHERE keyspace_name='ks' AND columnfamily_name='droptable'");
        if (rows.iterator().hasNext()) {
            fail("This table should have been deleted");
        }
    }

    @Test(groups = "short")
    public void should_create_an_index() {
        // Create a table
        session().execute(SchemaBuilder.createTable("ks", "CreateIndex")
                        .addPartitionKey("a", DataType.cint())
                        .addClusteringColumn("b", DataType.cint())
                        .addColumn("c", DataType.map(DataType.cint(), DataType.cint()))
        );

        // Create an index on a regular column of the table
        session().execute(SchemaBuilder.createIndex("ks_Index")
                        .onTable("ks", "CreateIndex")
                        .andColumn("b")
        );
        session().execute(SchemaBuilder.createIndex("ks_IndexOnMap")
                        .onTable("ks", "CreateIndex")
                        .andKeysOfColumn("c")
        );

        // Verify that the indexes exist on the right columns
        ResultSet rows = session().execute(
                "SELECT column_name, index_name, index_options, index_type, component_index "
                        + "FROM system.schema_columns "
                        + "WHERE keyspace_name='ks' "
                        + "AND columnfamily_name='createindex' "
                        + "AND column_name IN ('b', 'c')");
        Iterator<Row> iterator = rows.iterator();
        verifyNextIndexDefinition(iterator, "ks_Index", "{}", "COMPOSITES", 0);
        verifyNextIndexDefinition(iterator, "ks_IndexOnMap", "{\"index_keys\":\"\"}", "COMPOSITES", 1);
        assertThat(iterator.hasNext()).isFalse();
    }

    private void verifyNextIndexDefinition(Iterator<Row> iterator, String name, String options, String type,
                                           int index) {
        Row nextIndex = iterator.next();
        assertThat(nextIndex.getString("index_name")).isEqualTo(name);
        assertThat(nextIndex.getString("index_options")).isEqualTo(options);
        assertThat(nextIndex.getString("index_type")).isEqualTo(type);
        assertThat(nextIndex.getInt("component_index")).isEqualTo(index);
    }

    @Test(groups = "short")
    public void should_drop_an_index() {
        // Create a table
        session().execute(SchemaBuilder.createTable("ks", "DropIndex")
                        .addPartitionKey("a", DataType.cint())
                        .addClusteringColumn("b", DataType.cint())
        );

        // Create an index
        // Note: we have to pick a lower-case name because Cassandra uses the CamelCase index name at creation
        // but a lowercase index name at deletion
        // See : https://issues.apache.org/jira/browse/CASSANDRA-8365
        session().execute(SchemaBuilder.createIndex("ks_index")
                        .onTable("ks", "DropIndex")
                        .andColumn("b")
        );

        // Verify that the PK index and the secondary indexes both exist
        assertThat(numberOfIndexedColumns()).isEqualTo(1);

        // Delete the index
        session().execute(SchemaBuilder.dropIndex("ks", "ks_index"));

        // Verify that only the PK index exists
        assertThat(numberOfIndexedColumns()).isEqualTo(0);
    }

    private int numberOfIndexedColumns() {
        ResultSet columns = session().execute(
                "SELECT * "
                        + "FROM system.schema_columns "
                        + "WHERE keyspace_name='ks' "
                        + "AND columnfamily_name='dropindex' ");
        int count = 0;
        for (Row column : columns) {
            if (column.getString("index_name") != null)
                count += 1;
        }
        return count;
    }
}
