/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.Assertions.assertThat;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.createTable;
import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.udt;

import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder.RowsPerPartition;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy.CompactionWindowUnit;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.TimeWindowCompactionStrategy.TimestampResolution;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class CreateTableTest {

  @Test
  public void should_not_throw_on_toString_for_CreateTableStart() {
    assertThat(createTable("foo").toString()).isEqualTo("CREATE TABLE foo");
  }

  @Test
  public void should_generate_create_table_if_not_exists() {
    assertThat(createTable("bar").ifNotExists().withPartitionKey("k", DataTypes.INT))
        .hasCql("CREATE TABLE IF NOT EXISTS bar (k int PRIMARY KEY)");
  }

  @Test
  public void should_generate_create_table_with_single_partition_key() {
    assertThat(
            createTable("bar").withPartitionKey("k", DataTypes.INT).withColumn("v", DataTypes.TEXT))
        .hasCql("CREATE TABLE bar (k int PRIMARY KEY,v text)");
  }

  @Test
  public void should_generate_create_table_with_compound_partition_key() {
    assertThat(
            createTable("bar")
                .withPartitionKey("kc", DataTypes.INT)
                .withPartitionKey("ka", DataTypes.TIMESTAMP)
                .withColumn("v", DataTypes.TEXT))
        .hasCql("CREATE TABLE bar (kc int,ka timestamp,v text,PRIMARY KEY((kc,ka)))");
  }

  @Test
  public void should_generate_create_table_with_single_partition_key_and_clustering_column() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withClusteringColumn("c", DataTypes.TEXT)
                .withColumn("v", udt("val", true)))
        .hasCql("CREATE TABLE bar (k int,c text,v frozen<val>,PRIMARY KEY(k,c))");
  }

  @Test
  public void should_generate_create_table_with_static_column() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withClusteringColumn("c", DataTypes.TEXT)
                .withStaticColumn("s", DataTypes.TIMEUUID)
                .withColumn("v", udt("val", true)))
        .hasCql("CREATE TABLE bar (k int,c text,s timeuuid STATIC,v frozen<val>,PRIMARY KEY(k,c))");
  }

  @Test
  public void should_generate_create_table_with_compound_partition_key_and_clustering_columns() {
    assertThat(
            createTable("bar")
                .withPartitionKey("kc", DataTypes.INT)
                .withPartitionKey("ka", DataTypes.TIMESTAMP)
                .withClusteringColumn("c", DataTypes.FLOAT)
                .withClusteringColumn("a", DataTypes.UUID)
                .withColumn("v", DataTypes.TEXT))
        .hasCql(
            "CREATE TABLE bar (kc int,ka timestamp,c float,a uuid,v text,PRIMARY KEY((kc,ka),c,a))");
  }

  @Test
  public void should_generate_create_table_with_compact_storage() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCompactStorage())
        .hasCql("CREATE TABLE bar (k int PRIMARY KEY,v text) WITH COMPACT STORAGE");
  }

  @Test
  public void should_generate_create_table_with_clustering_single() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withClusteringColumn("c", DataTypes.TEXT)
                .withColumn("v", DataTypes.TEXT)
                .withClusteringOrder("c", ClusteringOrder.ASC))
        .hasCql(
            "CREATE TABLE bar (k int,c text,v text,PRIMARY KEY(k,c)) WITH CLUSTERING ORDER BY (c ASC)");
  }

  @Test
  public void should_generate_create_table_with_clustering_three() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withClusteringColumn("c0", DataTypes.TEXT)
                .withClusteringColumn("c1", DataTypes.TEXT)
                .withClusteringColumn("c2", DataTypes.TEXT)
                .withColumn("v", DataTypes.TEXT)
                .withClusteringOrder("c0", ClusteringOrder.DESC)
                .withClusteringOrder(
                    ImmutableMap.of("c1", ClusteringOrder.ASC, "c2", ClusteringOrder.DESC)))
        .hasCql(
            "CREATE TABLE bar (k int,c0 text,c1 text,c2 text,v text,PRIMARY KEY(k,c0,c1,c2)) WITH CLUSTERING ORDER BY (c0 DESC,c1 ASC,c2 DESC)");
  }

  @Test
  public void should_generate_create_table_with_compact_storage_and_default_ttl() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCompactStorage()
                .withDefaultTimeToLiveSeconds(86400))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH COMPACT STORAGE AND default_time_to_live=86400");
  }

  @Test
  public void should_generate_create_table_with_clustering_compact_storage_and_default_ttl() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withClusteringColumn("c", DataTypes.TEXT)
                .withColumn("v", DataTypes.TEXT)
                .withCompactStorage()
                .withClusteringOrder("c", ClusteringOrder.DESC)
                .withDefaultTimeToLiveSeconds(86400))
        .hasCql(
            "CREATE TABLE bar (k int,c text,v text,PRIMARY KEY(k,c)) WITH COMPACT STORAGE AND CLUSTERING ORDER BY (c DESC) AND default_time_to_live=86400");
  }

  @Test
  public void should_generate_create_table_with_options() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withBloomFilterFpChance(0.42)
                .withCDC(false)
                .withComment("Hello world")
                .withDcLocalReadRepairChance(0.54)
                .withDefaultTimeToLiveSeconds(86400)
                .withExtensions(
                    ImmutableMap.of(
                        "key1",
                        "apache".getBytes(StandardCharsets.UTF_8),
                        "key2",
                        "cassandra".getBytes(StandardCharsets.UTF_8)))
                .withGcGraceSeconds(864000)
                .withMemtableFlushPeriodInMs(10000)
                .withMinIndexInterval(1024)
                .withMaxIndexInterval(4096)
                .withReadRepairChance(0.55)
                .withSpeculativeRetry("99percentile"))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH bloom_filter_fp_chance=0.42 AND cdc=false AND comment='Hello world' AND dclocal_read_repair_chance=0.54 AND default_time_to_live=86400 AND extensions={'key1':0x617061636865,'key2':0x63617373616e647261} AND gc_grace_seconds=864000 AND memtable_flush_period_in_ms=10000 AND min_index_interval=1024 AND max_index_interval=4096 AND read_repair_chance=0.55 AND speculative_retry='99percentile'");
  }

  @Test
  public void should_generate_create_table_lz4_compression() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withLZ4Compression())
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'LZ4Compressor'}");
  }

  @Test
  public void should_generate_create_table_lz4_compression_options() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withLZ4Compression(1024, .5))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'LZ4Compressor','chunk_length_kb':1024,'crc_check_chance':0.5}");
  }

  @Test
  public void should_generate_create_table_snappy_compression() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withSnappyCompression())
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'SnappyCompressor'}");
  }

  @Test
  public void should_generate_create_table_snappy_compression_options() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withSnappyCompression(2048, .25))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'SnappyCompressor','chunk_length_kb':2048,'crc_check_chance':0.25}");
  }

  @Test
  public void should_generate_create_table_deflate_compression() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withDeflateCompression())
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'DeflateCompressor'}");
  }

  @Test
  public void should_generate_create_table_deflate_compression_options() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withDeflateCompression(4096, .1))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compression={'class':'DeflateCompressor','chunk_length_kb':4096,'crc_check_chance':0.1}");
  }

  @Test
  public void should_generate_create_table_caching_options() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCaching(true, RowsPerPartition.rows(10)))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH caching={'keys':'ALL','rows_per_partition':'10'}");
  }

  @Test
  public void should_generate_create_table_size_tiered_compaction() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCompaction(
                    SchemaBuilder.sizeTieredCompactionStrategy()
                        .withBucketHigh(1.6)
                        .withBucketLow(0.6)
                        .withColdReadsToOmit(0.1)
                        .withMaxThreshold(33)
                        .withMinThreshold(5)
                        .withMinSSTableSizeInBytes(50000)
                        .withOnlyPurgeRepairedTombstones(true)
                        .withEnabled(false)
                        .withTombstoneCompactionIntervalInSeconds(86400)
                        .withTombstoneThreshold(0.22)
                        .withUncheckedTombstoneCompaction(true)))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compaction={'class':'SizeTieredCompactionStrategy','bucket_high':1.6,'bucket_low':0.6,'cold_reads_to_omit':0.1,'max_threshold':33,'min_threshold':5,'min_sstable_size':50000,'only_purge_repaired_tombstones':true,'enabled':false,'tombstone_compaction_interval':86400,'tombstone_threshold':0.22,'unchecked_tombstone_compaction':true}");
  }

  @Test
  public void should_generate_create_table_leveled_compaction() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCompaction(
                    SchemaBuilder.leveledCompactionStrategy()
                        .withSSTableSizeInMB(110)
                        .withTombstoneCompactionIntervalInSeconds(3600)))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compaction={'class':'LeveledCompactionStrategy','sstable_size_in_mb':110,'tombstone_compaction_interval':3600}");
  }

  @Test
  public void should_generate_create_table_time_window_compaction() {
    assertThat(
            createTable("bar")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.TEXT)
                .withCompaction(
                    SchemaBuilder.timeWindowCompactionStrategy()
                        .withCompactionWindow(10, CompactionWindowUnit.DAYS)
                        .withTimestampResolution(TimestampResolution.MICROSECONDS)
                        .withUnsafeAggressiveSSTableExpiration(false)))
        .hasCql(
            "CREATE TABLE bar (k int PRIMARY KEY,v text) WITH compaction={'class':'TimeWindowCompactionStrategy','compaction_window_size':10,'compaction_window_unit':'DAYS','timestamp_resolution':'MICROSECONDS','unsafe_aggressive_sstable_expiration':false}");
  }

  @Test
  public void should_generate_vector_column() {
    assertThat(
            createTable("foo")
                .withPartitionKey("k", DataTypes.INT)
                .withColumn("v", DataTypes.vectorOf(DataTypes.FLOAT, 3)))
        .hasCql("CREATE TABLE foo (k int PRIMARY KEY,v vector<float, 3>)");
  }
}
