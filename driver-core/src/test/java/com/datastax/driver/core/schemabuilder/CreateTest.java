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

import com.datastax.driver.core.DataType;
import org.testng.annotations.Test;

import static com.datastax.driver.core.schemabuilder.SchemaBuilder.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Note: some addColumn variants are covered in {@link CreateTypeTest}.
 */
public class CreateTest {

    @Test(groups = "unit")
    public void should_create_simple_table() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_udt_partition_key() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addUDTPartitionKey("u", frozen("user"));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "u frozen<user>,\n\t\t" +
                "PRIMARY KEY(u))"
        );
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
    public void should_fail_when_creating_table_without_partition_key() throws Exception {
        createTable("test").addColumn("name", DataType.text()).getQueryString();
    }

    @Test(groups = "unit")
    public void should_create_simple_table_if_not_exists() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .ifNotExists()
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text());
        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE IF NOT EXISTS test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_simple_table_with_keyspace() throws Exception {
        //When
        SchemaStatement statement = createTable("ks", "test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text());
        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE ks.test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_simple_table_with_list() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.list(DataType.text()));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends list<text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_simple_table_with_set() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.set(DataType.text()));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends set<text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_simple_table_with_map() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.map(DataType.cint(), DataType.text()));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends map<int, text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_clustering_keys() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_udt_clustering_keys() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addUDTClusteringColumn("col2", frozen("address"))
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 frozen<address>,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_composite_partition_key() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id1", DataType.bigint())
                .addPartitionKey("id2", DataType.text())
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id1 bigint,\n\t\t" +
                "id2 text,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY((id1, id2)))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_composite_partition_key_and_clustering_keys() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id1", DataType.bigint())
                .addPartitionKey("id2", DataType.text())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id1 bigint,\n\t\t" +
                "id2 text,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY((id1, id2), col1, col2))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_static_column() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col", DataType.uuid())
                .addStaticColumn("bucket", DataType.cint())
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col uuid,\n\t\t" +
                "bucket int static,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_udt_static_column() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col", DataType.uuid())
                .addUDTStaticColumn("bucket", frozen("address"))
                .addColumn("name", DataType.text());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col uuid,\n\t\t" +
                "bucket frozen<address> static,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col))");
    }

    @Test(groups = "unit")
    public void should_create_table_with_clustering_order() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .clusteringOrder("col1", Direction.ASC)
                .clusteringOrder("col2", Direction.DESC);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))\n\t" +
                "WITH CLUSTERING ORDER BY(col1 ASC, col2 DESC)");
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_blank_clustering_order_column_provided() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions().clusteringOrder("", Direction.DESC);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_clustering_order_column_does_not_match_declared_clustering_keys() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions().clusteringOrder("col3", Direction.ASC);
    }

    @Test(groups = "unit")
    public void should_create_table_with_compact_storage() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .compactStorage();

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))\n\t" +
                "WITH COMPACT STORAGE");
    }

    @Test(groups = "unit")
    public void should_create_table_with_all_options() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringColumn("col1", DataType.uuid())
                .addClusteringColumn("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .clusteringOrder("col1", Direction.ASC)
                .clusteringOrder("col2", Direction.DESC)
                .compactStorage()
                .bloomFilterFPChance(0.01)
                .caching(Caching.ROWS_ONLY)
                .comment("This is a comment")
                .compactionOptions(leveledStrategy().ssTableSizeInMB(160))
                .compressionOptions(lz4())
                .dcLocalReadRepairChance(0.21)
                .defaultTimeToLive(100)
                .gcGraceSeconds(9999)
                .minIndexInterval(64)
                .maxIndexInterval(512)
                .memtableFlushPeriodInMillis(12)
                .populateIOCacheOnFlush(true)
                .readRepairChance(0.05)
                .replicateOnWrite(true)
                .speculativeRetry(always())
                .cdc(true);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\tPRIMARY KEY(id, col1, col2))\n\t" +
                "WITH caching = 'rows_only' " +
                "AND bloom_filter_fp_chance = 0.01 " +
                "AND comment = 'This is a comment' " +
                "AND compression = {'sstable_compression' : 'LZ4Compressor'} " +
                "AND compaction = {'class' : 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 160} " +
                "AND dclocal_read_repair_chance = 0.21 " +
                "AND default_time_to_live = 100 " +
                "AND gc_grace_seconds = 9999 " +
                "AND min_index_interval = 64 " +
                "AND max_index_interval = 512 " +
                "AND memtable_flush_period_in_ms = 12 " +
                "AND populate_io_cache_on_flush = true " +
                "AND read_repair_chance = 0.05 " +
                "AND replicate_on_write = true " +
                "AND speculative_retry = 'ALWAYS' " +
                "AND cdc = true AND CLUSTERING ORDER BY(col1 ASC, col2 DESC) AND COMPACT STORAGE");
    }

    @Test(groups = "unit")
    public void should_build_table_with_new_caching_options() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .caching(KeyCaching.ALL, rows(100));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\tPRIMARY KEY(id))\n\t" +
                "WITH caching = {'keys' : 'all', 'rows_per_partition' : 100}");

        //When
        statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .caching(KeyCaching.ALL, allRows());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\tPRIMARY KEY(id))\n\t" +
                "WITH caching = {'keys' : 'all', 'rows_per_partition' : 'all'}");
    }

    @Test(groups = "unit")
    public void should_build_table_with_custom_option() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .freeformOption("key1", "value1")
                .freeformOption("key2", 1.0);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\tPRIMARY KEY(id))\n\t" +
                "WITH key1 = 'value1' " +
                "AND key2 = 1.0");
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class)
    public void should_fail_when_both_caching_versions() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .caching(Caching.KEYS_ONLY)
                .caching(KeyCaching.ALL, allRows()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_negative_rows_per_partition() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .caching(KeyCaching.ALL, rows(-3));
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_read_repair_chance_out_of_bound() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .readRepairChance(1.3);
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class)
    public void should_fail_when_read_repair_chance_negative() throws Exception {
        createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .readRepairChance(-1.3);
    }

    @Test(groups = "unit")
    public void should_create_table_with_speculative_retry_none() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(noSpeculativeRetry());

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = 'NONE'");
    }

    @Test(groups = "unit")
    public void should_create_table_with_speculative_retry_in_percentile() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(percentile(95));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = '95percentile'");
    }

    @Test(groups = "unit")
    public void should_create_table_with_speculative_retry_in_milli_secs() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(millisecs(12));

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = '12ms'");
    }

    @Test(groups = "unit")
    public void should_create_table_with_cdc_true() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .cdc(true);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH cdc = true");
    }

    @Test(groups = "unit")
    public void should_create_table_with_cdc_false() throws Exception {
        //When
        SchemaStatement statement = createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .cdc(false);

        //Then
        assertThat(statement.getQueryString()).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH cdc = false");
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[pk\\]' columns can not be declared as partition keys and clustering keys at the same time")
    public void should_fail_if_same_partition_and_clustering_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("pk", DataType.bigint()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[pk\\]' columns can not be declared as partition keys and simple columns at the same time")
    public void should_fail_if_same_partition_and_simple_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addColumn("pk", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[cluster\\]' columns can not be declared as clustering keys and simple columns at the same time")
    public void should_fail_if_same_clustering_and_simple_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.bigint())
                .addColumn("cluster", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[pk\\]' columns can not be declared as partition keys and static columns at the same time")
    public void should_fail_if_same_partition_and_static_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addStaticColumn("pk", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[cluster\\]' columns can not be declared as clustering keys and static columns at the same time")
    public void should_fail_if_same_clustering_and_static_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.bigint())
                .addStaticColumn("cluster", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The '\\[col\\]' columns can not be declared as simple columns and static columns at the same time")
    public void should_fail_if_same_simple_and_static_column() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.uuid())
                .addColumn("col", DataType.bigint())
                .addStaticColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "The table 'test' cannot declare static columns '\\[stat\\]' without clustering columns")
    public void should_fail_if_static_column_in_non_clustered_table() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addStaticColumn("stat", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The keyspace name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        createTable("ADD", "test")
                .addPartitionKey("pk", DataType.bigint())
                .addColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The table name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        createTable("ADD")
                .addPartitionKey("pk", DataType.bigint())
                .addColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The partition key name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_partition_key_is_a_reserved_keyword() throws Exception {
        createTable("test")
                .addPartitionKey("ADD", DataType.bigint())
                .addColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The clustering column name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_clustering_key_is_a_reserved_keyword() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("ADD", DataType.uuid())
                .addColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The column name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_simple_column_is_a_reserved_keyword() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.uuid())
                .addColumn("ADD", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = "The static column name 'ADD' is not allowed because it is a reserved keyword")
    public void should_fail_if_static_column_is_a_reserved_keyword() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.uuid())
                .addStaticColumn("ADD", DataType.text())
                .addColumn("col", DataType.text()).getQueryString();
    }

    @Test(groups = "unit", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Cannot create table 'test' with compact storage and static columns '\\[stat\\]'")
    public void should_fail_creating_table_with_static_columns_and_compact_storage() throws Exception {
        createTable("test")
                .addPartitionKey("pk", DataType.bigint())
                .addClusteringColumn("cluster", DataType.uuid())
                .addStaticColumn("stat", DataType.text())
                .withOptions().compactStorage().getQueryString();
    }
}
