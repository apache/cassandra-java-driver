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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, name text, PRIMARY KEY(id))");
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
        assertThat(built).isEqualTo("CREATE TABLE IF NOT EXISTS test(id bigint, name text, PRIMARY KEY(id))");
    }

    @Test
    public void should_create_simple_table_with_keyspace() throws Exception {
        //When
        final String built = SchemaBuilder.createTable("ks", "test")
                .addPartitionKey("id", DataType.bigint())
                .addColumn("name", DataType.text())
                .build();
        //Then
        assertThat(built).isEqualTo("CREATE TABLE ks.test(id bigint, name text, PRIMARY KEY(id))");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, col1 uuid, col2 uuid, name text, PRIMARY KEY(id, col1, col2))");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id1 bigint, id2 text, name text, PRIMARY KEY((id1, id2)))");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id1 bigint, id2 text, col1 uuid, col2 uuid, name text, PRIMARY KEY((id1, id2), col1, col2))");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, col1 uuid, col2 uuid, name text, PRIMARY KEY(id, col1, col2)) WITH CLUSTERING ORDER BY(col1 ASC, col2 DESC)");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, col1 uuid, col2 uuid, name text, PRIMARY KEY(id, col1, col2)) WITH COMPACT STORAGE");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, col1 uuid, col2 uuid, name text, PRIMARY KEY(id, col1, col2)) " +
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, name text, PRIMARY KEY(id)) WITH speculative_retry = 'NONE'");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, name text, PRIMARY KEY(id)) WITH speculative_retry = '95percentile'");
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
        assertThat(built).isEqualTo("CREATE TABLE test(id bigint, name text, PRIMARY KEY(id)) WITH speculative_retry = '12ms'");
    }

}
