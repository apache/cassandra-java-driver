package com.datastax.driver.core.schemabuilder;

import static com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder.Sorting.ASC;
import static com.datastax.driver.core.schemabuilder.Create.Options.ClusteringOrder.Sorting.DESC;
import static com.datastax.driver.core.schemabuilder.TableOptions.Caching.ROWS_ONLY;
import static org.assertj.core.api.Assertions.assertThat;
import org.testng.annotations.Test;
import com.datastax.driver.core.DataType;

public class CreateTest {

    @Test
    public void should_create_simple_table() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void should_exception_when_creating_table_without_partition_key() throws Exception {
        SchemaBuilder.createTable("test").addColumn("name", DataType.text()).build();
    }

    @Test
    public void should_create_simple_table_if_not_exists() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .ifNotExists(true)
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE IF NOT EXISTS test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test
    public void should_create_simple_table_with_keyspace() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("ks", "test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE ks.test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))");
    }


    @Test
    public void should_create_simple_table_with_list() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.list(DataType.text()))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends list<text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test
    public void should_create_simple_table_with_set() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.set(DataType.text()))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends set<text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test
    public void should_create_simple_table_with_map() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("friends", DataType.map(DataType.cint(), DataType.text()))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "friends map<int,text>,\n\t\t" +
                "PRIMARY KEY(id))");
    }

    @Test
    public void should_create_table_with_clustering_keys() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))");
    }

    @Test
    public void should_create_table_with_composite_partition_key() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id1", DataType.bigint())
                .addPartitionKey("id2", DataType.text())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id1 bigint,\n\t\t" +
                "id2 text,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY((id1, id2)))");
    }

    @Test
    public void should_create_table_with_composite_partition_key_and_clustering_keys() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id1", DataType.bigint())
                .addPartitionKey("id2", DataType.text())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id1 bigint,\n\t\t" +
                "id2 text,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY((id1, id2), col1, col2))");
    }

    @Test
    public void should_create_table_with_static_column() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col", DataType.uuid())
                .addStaticColumn("bucket", DataType.cint())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col uuid,\n\t\t" +
                "bucket int static,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col))");
    }

    @Test
    public void should_create_table_with_clustering_order() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .clusteringOrder(new Create.Options.ClusteringOrder("col1", ASC), new Create.Options.ClusteringOrder("col2", DESC))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))\n\t" +
                "WITH CLUSTERING ORDER BY(col1 ASC, col2 DESC)");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_exception_when_no_clustering_order_provided() throws Exception {
        SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions().clusteringOrder().build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_exception_when_blank_clustering_order_column_provided() throws Exception {
        SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions().clusteringOrder(new Create.Options.ClusteringOrder("", DESC)).build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_exception_when_clustering_order_column_does_not_match_declared_clustering_keys() throws Exception {
        SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions().clusteringOrder(new Create.Options.ClusteringOrder("col3", ASC)).build();
    }

    @Test
    public void should_create_table_with_compact_storage() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .compactStorage(true)
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "col1 uuid,\n\t\t" +
                "col2 uuid,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id, col1, col2))\n\t" +
                "WITH COMPACT STORAGE");
    }

    @Test
    public void should_create_table_with_all_options() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addClusteringKey("col1", DataType.uuid())
                .addClusteringKey("col2", DataType.uuid())
                .addColumn("name", DataType.text())
                .withOptions()
                .clusteringOrder(new Create.Options.ClusteringOrder("col1", ASC), new Create.Options.ClusteringOrder("col2", DESC))
                .compactStorage(true)
                .bloomFilterFPChance(0.01)
                .caching(ROWS_ONLY)
                .comment("This is a comment")
                .compactionOptions(TableOptions.CompactionOptions.leveledStrategy().ssTableSizeInMB(160))
                .compressionOptions(TableOptions.CompressionOptions.lz4())
                .dcLocalReadRepairChance(0.21)
                .defaultTimeToLive(100)
                .gcGraceSeconds(9999L)
                .indexInterval(512)
                .memtableFlushPeriodInMillis(12L)
                .populateIOOnCacheFlush(true)
                .replicateOnWrite(true)
                .speculativeRetry(TableOptions.SpeculativeRetryValue.always())
                .build();

        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
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
                "AND index_interval = 512 " +
                "AND memtable_flush_period_in_ms = 12 " +
                "AND populate_io_cache_on_flush = true " +
                "AND replicate_on_write = true " +
                "AND speculative_retry = 'ALWAYS' AND CLUSTERING ORDER BY(col1 ASC, col2 DESC) AND COMPACT STORAGE");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_exception_when_read_repair_chance_out_of_bound() throws Exception {
        SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .readRepairChance(1.3)
                .build();
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void should_exception_when_read_repair_chance_negative() throws Exception {
        SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .readRepairChance(-1.3)
                .build();
    }

    @Test
    public void should_create_table_with_speculative_retry_none() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(TableOptions.SpeculativeRetryValue.none())
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = 'NONE'");
    }

    @Test
    public void should_create_table_with_speculative_retry_in_percentile() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(TableOptions.SpeculativeRetryValue.percentile(95))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = '95percentile'");
    }

    @Test
    public void should_create_table_with_speculative_retry_in_milli_secs() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .withOptions()
                .speculativeRetry(TableOptions.SpeculativeRetryValue.millisecs(12))
                .build();
        //Then
        assertThat(built).isEqualTo("\n\tCREATE TABLE test(\n\t\t" +
                "id bigint,\n\t\t" +
                "name text,\n\t\t" +
                "PRIMARY KEY(id))\n\t" +
                "WITH speculative_retry = '12ms'");
    }

    @Test
    public void should_fail_if_same_partition_and_clustering_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("pk", DataType.bigint())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[pk]' columns can not be declared as partition keys and clustering keys at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_same_partition_and_simple_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addColumn("pk", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[pk]' columns can not be declared as partition keys and simple columns at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_same_clustering_and_simple_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster", DataType.bigint())
                    .addColumn("cluster", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[cluster]' columns can not be declared as clustering keys and simple columns at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_same_partition_and_static_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addStaticColumn("pk", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[pk]' columns can not be declared as partition keys and static columns at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_same_clustering_and_static_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster", DataType.bigint())
                    .addStaticColumn("cluster", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[cluster]' columns can not be declared as clustering keys and static columns at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_same_simple_and_static_column() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster",DataType.uuid())
                    .addColumn("col", DataType.bigint())
                    .addStaticColumn("col", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The '[col]' columns can not be declared as simple columns and static columns at the same time");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_static_column_in_non_clustered_table() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addStaticColumn("stat", DataType.text())
                    .build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The table 'test' cannot declare static columns '[stat]' without clustering columns");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_keyspace_name_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("add","test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addColumn("col", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The keyspace name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }


    @Test
    public void should_fail_if_table_name_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("add")
                    .addPartitionKey("pk", DataType.bigint())
                    .addColumn("col", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The table name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_partition_key_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("add", DataType.bigint())
                    .addColumn("col", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The partition key name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_clustering_key_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("add", DataType.uuid())
                    .addColumn("col", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The clustering key name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_simple_column_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster", DataType.uuid())
                    .addColumn("add", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The column name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_if_static_column_is_a_reserved_keyword() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster", DataType.uuid())
                    .addStaticColumn("add", DataType.text())
                    .addColumn("col", DataType.text())
                    .build();
        } catch (IllegalArgumentException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("The static column name 'add' is not allowed because it is a reserved keyword");
        }

        assertThat(hasFailed).isTrue();
    }

    @Test
    public void should_fail_creating_table_with_static_columns_and_compact_storage() throws Exception {
        boolean hasFailed = false;
        try {
            SchemaBuilder.createTable("test")
                    .addPartitionKey("pk", DataType.bigint())
                    .addClusteringKey("cluster", DataType.uuid())
                    .addStaticColumn("stat", DataType.text())
                    .withOptions().compactStorage(true).build();
        } catch (IllegalStateException e) {
            hasFailed = true;
            assertThat(e.getMessage()).isEqualTo("Cannot create table 'test' with compact storage and static columns '[stat]'");
        }

        assertThat(hasFailed).isTrue();
    }

}
