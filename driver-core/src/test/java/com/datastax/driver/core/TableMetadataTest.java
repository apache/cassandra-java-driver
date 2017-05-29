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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ClusteringOrder.ASC;
import static com.datastax.driver.core.ClusteringOrder.DESC;
import static com.datastax.driver.core.DataType.*;
import static org.assertj.core.api.Assertions.entry;

@CCMConfig(clusterProvider = "createClusterBuilderNoDebouncing")
public class TableMetadataTest extends CCMTestsSupport {

    @Test(groups = "short")
    public void should_parse_table_without_clustering_columns() {
        // given
        String cql = String.format("CREATE TABLE %s.static (\n"
                + "    k text,\n"
                + "    i int,\n"
                + "    m map<text, timeuuid>,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k)\n"
                + ");", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("static");
        // then
        assertThat(table).isNotNull().hasName("static").hasNumberOfColumns(4).isNotCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("i").isRegularColumn().hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("m").isRegularColumn().hasType(map(text(), timeuuid()));
        assertThat(table.getColumns().get(3)).isNotNull().hasName("v").isRegularColumn().hasType(cint());
    }

    @Test(groups = "short")
    public void should_parse_table_with_clustering_columns() {
        // given
        String cql = String.format("CREATE TABLE %s.sparse (\n"
                + "    k text,\n"
                + "    c1 int,\n"
                + "    c2 float,\n"
                + "    l list<text>,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k, c1, c2)\n"
                + ") WITH CLUSTERING ORDER BY (c1 ASC, c2 DESC);", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("sparse");
        // then
        assertThat(table).isNotNull().hasName("sparse").hasNumberOfColumns(5).isNotCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c1").isClusteringColumn().hasClusteringOrder(ASC).hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("c2").isClusteringColumn().hasClusteringOrder(DESC).hasType(cfloat());
        assertThat(table.getColumns().get(3)).isNotNull().hasName("l").isRegularColumn().hasType(list(text()));
        assertThat(table.getColumns().get(4)).isNotNull().hasName("v").isRegularColumn().hasType(cint());
    }

    @Test(groups = "short")
    public void should_parse_counter_table() {
        // given
        String cql = String.format("CREATE TABLE %s.counters (\n"
                + "    k text,\n"
                + "    c counter,\n"
                + "    PRIMARY KEY (k)\n"
                + ");", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("counters");
        // then
        assertThat(table).isNotNull().hasName("counters").hasNumberOfColumns(2).isNotCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c").isRegularColumn().hasType(counter());
    }

    @Test(groups = "short")
    public void should_parse_compact_static_table() {
        // given
        String cql = String.format("CREATE TABLE %s.compact_static (\n"
                + "    k text,\n"
                + "    i int,\n"
                + "    t timeuuid,\n"
                + "    v int,\n"
                + "    PRIMARY KEY (k)\n"
                + ") WITH COMPACT STORAGE;", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("compact_static");
        // then
        assertThat(table).isNotNull().hasName("compact_static").hasNumberOfColumns(4).isCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("i").isRegularColumn().hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("t").isRegularColumn().hasType(timeuuid());
        assertThat(table.getColumns().get(3)).isNotNull().hasName("v").isRegularColumn().hasType(cint());
    }

    @Test(groups = "short")
    public void should_parse_dense_table() {
        // given
        String cql = String.format("CREATE TABLE %s.dense (\n"
                + "        k int,\n"
                + "        c int,\n"
                + "        PRIMARY KEY (k, c)\n"
                + "    ) WITH COMPACT STORAGE;", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("dense");
        // then
        assertThat(table).isNotNull().hasName("dense").hasNumberOfColumns(2).isCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(cint());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c").isClusteringColumn().hasType(cint());
    }

    @Test(groups = "short")
    public void should_parse_compact_dynamic_table() {
        // given
        String cql = String.format("CREATE TABLE %s.compact_dynamic (\n"
                + "    k text,\n"
                + "    c int,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c)\n"
                + ") WITH COMPACT STORAGE;", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("compact_dynamic");
        // then
        assertThat(table).isNotNull().hasName("compact_dynamic").hasNumberOfColumns(3).isCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c").isClusteringColumn().hasClusteringOrder(ASC).hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("v").isRegularColumn().hasType(timeuuid());
    }

    @Test(groups = "short")
    public void should_parse_compact_table_with_multiple_clustering_columns() {
        // given
        String cql = String.format("CREATE TABLE %s.compact_composite (\n"
                + "    k text,\n"
                + "    c1 int,\n"
                + "    c2 float,\n"
                + "    c3 double,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c1, c2, c3)\n"
                + ") WITH COMPACT STORAGE;", keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("compact_composite");
        // then
        assertThat(table).isNotNull().hasName("compact_composite").hasNumberOfColumns(5).isCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c1").isClusteringColumn().hasClusteringOrder(ASC).hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("c2").isClusteringColumn().hasClusteringOrder(ASC).hasType(cfloat());
        assertThat(table.getColumns().get(3)).isNotNull().hasName("c3").isClusteringColumn().hasClusteringOrder(ASC).hasType(cdouble());
        assertThat(table.getColumns().get(4)).isNotNull().hasName("v").isRegularColumn().hasType(timeuuid());
    }

    @Test(groups = "short")
    public void should_parse_table_options() {
        VersionNumber version = TestUtils.findHost(cluster(), 1).getCassandraVersion();

        // given
        String cql;

        // Cassandra 3.0 +
        if (version.getMajor() > 2) {
            cql = String.format("CREATE TABLE %s.with_options (\n"
                            + "    k text,\n"
                            + "    c1 int,\n"
                            + "    c2 int,\n"
                            + "    i int,\n"
                            + "    PRIMARY KEY (k, c1, c2)\n"
                            + ") WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)\n"
                            + "   AND read_repair_chance = 0.5\n"
                            + "   AND dclocal_read_repair_chance = 0.6\n"
                            + "   AND speculative_retry = '99.9PERCENTILE'\n"
                            // replicate_on_write not supported anymore in 3.0
                            + "   AND gc_grace_seconds = 42\n"
                            + "   AND bloom_filter_fp_chance = 0.01\n"
                            // older caching formats not supported anymore in 3.0
                            + "   AND caching =  { 'keys' : 'ALL', 'rows_per_partition' : 10 }\n"
                            + "   AND comment = 'My awesome table'\n"
                            + "   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 15 }\n"
                            + "   AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor', 'chunk_length_kb' : 128 }\n"
                            + "   AND crc_check_chance = 0.5;", // available from C* 3.0
                    keyspace);

            // older versions
        } else {
            cql = String.format("CREATE TABLE %s.with_options (\n"
                            + "    k text,\n"
                            + "    c1 int,\n"
                            + "    c2 int,\n"
                            + "    i int,\n"
                            + "    PRIMARY KEY (k, c1, c2)\n"
                            + ") WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)\n"
                            + "   AND read_repair_chance = 0.5\n"
                            + "   AND dclocal_read_repair_chance = 0.6\n"
                            + "   AND replicate_on_write = true\n"
                            + "   AND gc_grace_seconds = 42\n"
                            + "   AND bloom_filter_fp_chance = 0.01\n"
                            + "   AND caching = 'ALL'\n"
                            + "   AND comment = 'My awesome table'\n"
                            + "   AND compaction = { 'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'sstable_size_in_mb' : 15 }\n"
                            + "   AND compression = { 'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor', 'chunk_length_kb' : 128 };",
                    keyspace);
        }

        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("with_options");

        // then
        assertThat(table).isNotNull().hasName("with_options").hasNumberOfColumns(4).isNotCompactStorage();
        assertThat(table.getColumns().get(0)).isNotNull().hasName("k").isPartitionKey().hasType(text());
        assertThat(table.getColumns().get(1)).isNotNull().hasName("c1").isClusteringColumn().hasClusteringOrder(DESC).hasType(cint());
        assertThat(table.getColumns().get(2)).isNotNull().hasName("c2").isClusteringColumn().hasClusteringOrder(ASC).hasType(cint());
        assertThat(table.getColumns().get(3)).isNotNull().hasName("i").isRegularColumn().hasType(cint());
        assertThat(table);

        // Cassandra 3.8 +
        if (version.getMajor() > 3 || (version.getMajor() == 3 && version.getMinor() >= 8)) {

            assertThat(table.getOptions().getReadRepairChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getLocalReadRepairChance()).isEqualTo(0.6);
            assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(42);
            assertThat(table.getOptions().getBloomFilterFalsePositiveChance()).isEqualTo(0.01);
            assertThat(table.getOptions().getComment()).isEqualTo("My awesome table");
            assertThat(table.getOptions().getCaching()).contains(entry("keys", "ALL"));
            assertThat(table.getOptions().getCaching()).contains(entry("rows_per_partition", "10"));
            assertThat(table.getOptions().getCompaction()).contains(entry("class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"));
            assertThat(table.getOptions().getCompaction()).contains(entry("sstable_size_in_mb", "15"));
            assertThat(table.getOptions().getCompression()).contains(entry("class", "org.apache.cassandra.io.compress.SnappyCompressor")); // sstable_compression becomes class
            assertThat(table.getOptions().getCompression()).contains(entry("chunk_length_in_kb", "128")); // note the "in" prefix
            assertThat(table.getOptions().getDefaultTimeToLive()).isEqualTo(0);
            assertThat(table.getOptions().getSpeculativeRetry()).isEqualTo("99.9PERCENTILE");
            assertThat(table.getOptions().getIndexInterval()).isNull();
            assertThat(table.getOptions().getMinIndexInterval()).isEqualTo(128);
            assertThat(table.getOptions().getMaxIndexInterval()).isEqualTo(2048);
            assertThat(table.getOptions().getReplicateOnWrite()).isTrue(); // default
            assertThat(table.getOptions().getCrcCheckChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getExtensions()).isEmpty(); // default
            assertThat(table.asCQLQuery())
                    .contains("read_repair_chance = 0.5")
                    .contains("dclocal_read_repair_chance = 0.6")
                    .contains("gc_grace_seconds = 42")
                    .contains("bloom_filter_fp_chance = 0.01")
                    .contains("comment = 'My awesome table'")
                    .contains("'keys' : 'ALL'")
                    .contains("'rows_per_partition' : 10")
                    .contains("'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'")
                    .contains("'sstable_size_in_mb' : 15")
                    .contains("'class' : 'org.apache.cassandra.io.compress.SnappyCompressor'") // sstable_compression becomes class
                    .contains("'chunk_length_in_kb' : 128") // note the "in" prefix
                    .contains("default_time_to_live = 0")
                    .contains("speculative_retry = '99.9PERCENTILE'")
                    .contains("min_index_interval = 128")
                    .contains("max_index_interval = 2048")
                    .contains("crc_check_chance = 0.5")
                    .contains("cdc = false")
                    .doesNotContain(" index_interval")
                    .doesNotContain("replicate_on_write");
            // Cassandra 3.0 +
        } else if (version.getMajor() > 2) {

            assertThat(table.getOptions().getReadRepairChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getLocalReadRepairChance()).isEqualTo(0.6);
            assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(42);
            assertThat(table.getOptions().getBloomFilterFalsePositiveChance()).isEqualTo(0.01);
            assertThat(table.getOptions().getComment()).isEqualTo("My awesome table");
            assertThat(table.getOptions().getCaching()).contains(entry("keys", "ALL"));
            assertThat(table.getOptions().getCaching()).contains(entry("rows_per_partition", "10"));
            assertThat(table.getOptions().getCompaction()).contains(entry("class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"));
            assertThat(table.getOptions().getCompaction()).contains(entry("sstable_size_in_mb", "15"));
            assertThat(table.getOptions().getCompression()).contains(entry("class", "org.apache.cassandra.io.compress.SnappyCompressor")); // sstable_compression becomes class
            assertThat(table.getOptions().getCompression()).contains(entry("chunk_length_in_kb", "128")); // note the "in" prefix
            assertThat(table.getOptions().getDefaultTimeToLive()).isEqualTo(0);
            assertThat(table.getOptions().getSpeculativeRetry()).isEqualTo("99.9PERCENTILE");
            assertThat(table.getOptions().getIndexInterval()).isNull();
            assertThat(table.getOptions().getMinIndexInterval()).isEqualTo(128);
            assertThat(table.getOptions().getMaxIndexInterval()).isEqualTo(2048);
            assertThat(table.getOptions().getReplicateOnWrite()).isTrue(); // default
            assertThat(table.getOptions().getCrcCheckChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getExtensions()).isEmpty(); // default
            assertThat(table.asCQLQuery())
                    .contains("read_repair_chance = 0.5")
                    .contains("dclocal_read_repair_chance = 0.6")
                    .contains("gc_grace_seconds = 42")
                    .contains("bloom_filter_fp_chance = 0.01")
                    .contains("comment = 'My awesome table'")
                    .contains("'keys' : 'ALL'")
                    .contains("'rows_per_partition' : 10")
                    .contains("'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'")
                    .contains("'sstable_size_in_mb' : 15")
                    .contains("'class' : 'org.apache.cassandra.io.compress.SnappyCompressor'") // sstable_compression becomes class
                    .contains("'chunk_length_in_kb' : 128") // note the "in" prefix
                    .contains("default_time_to_live = 0")
                    .contains("speculative_retry = '99.9PERCENTILE'")
                    .contains("min_index_interval = 128")
                    .contains("max_index_interval = 2048")
                    .contains("crc_check_chance = 0.5")
                    .doesNotContain(" index_interval")
                    .doesNotContain("replicate_on_write")
                    .doesNotContain("cdc"); // 3.8+

            // Cassandra 2.1 and 2.2
        } else if (version.getMajor() == 2 && version.getMinor() > 0) {

            // With 2.1 we have different options, the caching option changes and replicate_on_write disappears
            assertThat(table.getOptions().getReadRepairChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getLocalReadRepairChance()).isEqualTo(0.6);
            assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(42);
            assertThat(table.getOptions().getBloomFilterFalsePositiveChance()).isEqualTo(0.01);
            assertThat(table.getOptions().getComment()).isEqualTo("My awesome table");
            assertThat(table.getOptions().getCaching()).contains(entry("keys", "ALL"));
            assertThat(table.getOptions().getCaching()).contains(entry("rows_per_partition", "ALL"));
            assertThat(table.getOptions().getCompaction()).contains(entry("class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"));
            assertThat(table.getOptions().getCompaction()).contains(entry("sstable_size_in_mb", "15"));
            assertThat(table.getOptions().getCompression()).contains(entry("sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor"));
            assertThat(table.getOptions().getCompression()).contains(entry("chunk_length_kb", "128"));
            assertThat(table.getOptions().getDefaultTimeToLive()).isEqualTo(0);
            assertThat(table.getOptions().getSpeculativeRetry()).isEqualTo("99.0PERCENTILE");
            assertThat(table.getOptions().getIndexInterval()).isNull();
            assertThat(table.getOptions().getMinIndexInterval()).isEqualTo(128);
            assertThat(table.getOptions().getMaxIndexInterval()).isEqualTo(2048);
            assertThat(table.getOptions().getReplicateOnWrite()).isTrue(); // default
            assertThat(table.getOptions().getExtensions()).isEmpty();
            assertThat(table.asCQLQuery())
                    .contains("read_repair_chance = 0.5")
                    .contains("dclocal_read_repair_chance = 0.6")
                    .contains("gc_grace_seconds = 42")
                    .contains("bloom_filter_fp_chance = 0.01")
                    .contains("comment = 'My awesome table'")
                    .contains("'keys' : 'ALL'")
                    .contains("'rows_per_partition' : 'ALL'")
                    .contains("'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'")
                    .contains("'sstable_size_in_mb' : 15")
                    .contains("'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'")
                    .contains("'chunk_length_kb' : 128")
                    .contains("default_time_to_live = 0")
                    .contains("speculative_retry = '99.0PERCENTILE'")
                    .contains("min_index_interval = 128")
                    .contains("max_index_interval = 2048")
                    .doesNotContain(" index_interval")
                    .doesNotContain("replicate_on_write")
                    .doesNotContain("cdc");

            // Cassandra 2.0
        } else if (version.getMajor() == 2 && version.getMinor() == 0) {

            assertThat(table.getOptions().getReadRepairChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getLocalReadRepairChance()).isEqualTo(0.6);
            assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(42);
            assertThat(table.getOptions().getBloomFilterFalsePositiveChance()).isEqualTo(0.01);
            assertThat(table.getOptions().getComment()).isEqualTo("My awesome table");
            assertThat(table.getOptions().getCaching()).contains(entry("keys", "ALL"));
            assertThat(table.getOptions().getCaching()).doesNotContain(entry("rows_per_partition", "ALL")); // 2.1 +
            assertThat(table.getOptions().getCompaction()).contains(entry("class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"));
            assertThat(table.getOptions().getCompaction()).contains(entry("sstable_size_in_mb", "15"));
            assertThat(table.getOptions().getCompression()).contains(entry("sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor"));
            assertThat(table.getOptions().getCompression()).contains(entry("chunk_length_kb", "128"));
            assertThat(table.getOptions().getDefaultTimeToLive()).isEqualTo(0);
            assertThat(table.getOptions().getSpeculativeRetry()).isEqualTo("99.0PERCENTILE"); // default
            assertThat(table.getOptions().getIndexInterval()).isEqualTo(128);
            assertThat(table.getOptions().getMinIndexInterval()).isNull();
            assertThat(table.getOptions().getMaxIndexInterval()).isNull();
            assertThat(table.getOptions().getReplicateOnWrite()).isTrue(); // explicitly set
            assertThat(table.getOptions().getExtensions()).isEmpty();
            assertThat(table.asCQLQuery())
                    .contains("read_repair_chance = 0.5")
                    .contains("dclocal_read_repair_chance = 0.6")
                    .contains("gc_grace_seconds = 42")
                    .contains("bloom_filter_fp_chance = 0.01")
                    .contains("comment = 'My awesome table'")
                    .contains("caching = 'ALL'")
                    .contains("'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'")
                    .contains("'sstable_size_in_mb' : 15")
                    .contains("'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'")
                    .contains("'chunk_length_kb' : 128")
                    .contains("replicate_on_write = true")
                    .contains("index_interval = 128")
                    .contains("speculative_retry = '99.0PERCENTILE'")
                    .contains("default_time_to_live = 0")
                    .doesNotContain("min_index_interval") // 2.1 +
                    .doesNotContain("max_index_interval") // 2.1 +
                    .doesNotContain("cdc");

            // Cassandra 1.2
        } else {

            assertThat(table.getOptions().getReadRepairChance()).isEqualTo(0.5);
            assertThat(table.getOptions().getLocalReadRepairChance()).isEqualTo(0.6);
            assertThat(table.getOptions().getGcGraceInSeconds()).isEqualTo(42);
            assertThat(table.getOptions().getBloomFilterFalsePositiveChance()).isEqualTo(0.01);
            assertThat(table.getOptions().getComment()).isEqualTo("My awesome table");
            assertThat(table.getOptions().getCaching()).contains(entry("keys", "ALL"));
            assertThat(table.getOptions().getCaching()).doesNotContain(entry("rows_per_partition", "ALL")); // 2.1 +
            assertThat(table.getOptions().getCompaction()).contains(entry("class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"));
            assertThat(table.getOptions().getCompaction()).contains(entry("sstable_size_in_mb", "15"));
            assertThat(table.getOptions().getCompression()).contains(entry("sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor"));
            assertThat(table.getOptions().getCompression()).contains(entry("chunk_length_kb", "128"));
            assertThat(table.getOptions().getDefaultTimeToLive()).isEqualTo(0); // default
            assertThat(table.getOptions().getSpeculativeRetry()).isEqualTo("NONE"); // default
            assertThat(table.getOptions().getIndexInterval()).isNull();
            assertThat(table.getOptions().getMinIndexInterval()).isNull();
            assertThat(table.getOptions().getMaxIndexInterval()).isNull();
            assertThat(table.getOptions().getReplicateOnWrite()).isTrue(); // explicitly set
            assertThat(table.getOptions().getExtensions()).isEmpty();
            assertThat(table.asCQLQuery())
                    .contains("read_repair_chance = 0.5")
                    .contains("dclocal_read_repair_chance = 0.6")
                    .contains("gc_grace_seconds = 42")
                    .contains("bloom_filter_fp_chance = 0.01")
                    .contains("comment = 'My awesome table'")
                    .contains("caching = 'ALL'")
                    .contains("'class' : 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'")
                    .contains("'sstable_size_in_mb' : 15")
                    .contains("'sstable_compression' : 'org.apache.cassandra.io.compress.SnappyCompressor'")
                    .contains("'chunk_length_kb' : 128")
                    .contains("replicate_on_write = true")
                    .doesNotContain("index_interval")  // 2.0
                    .doesNotContain("min_index_interval")  // 2.1 +
                    .doesNotContain("max_index_interval")  // 2.1 +
                    .doesNotContain("speculative_retry")  // 2.0 +
                    .doesNotContain("default_time_to_live") // 2.0 +
                    .doesNotContain("cdc");

        }

    }

    /**
     * Validates that metadata is appropriately parsed when using the new compression options format introduced
     * in 3.0.0-alpha1.   Since compression options are parsed as a String map, this should behave exactly as
     * the old style compression options used in {@link #should_parse_table_options}.
     *
     * @jira_ticket CASSANDRA-9424
     */
    @Test(groups = "short")
    @CassandraVersion("3.0")
    public void should_parse_new_compression_options() {
        // given
        String cql = String.format("CREATE TABLE %s.new_compression_options (\n"
                        + "    k text,\n"
                        + "    c1 int,\n"
                        + "    c2 int,\n"
                        + "    i int,\n"
                        + "    PRIMARY KEY (k, c1, c2)\n"
                        + ") WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)\n"
                        + "   AND compression = { 'class' : 'DeflateCompressor', 'chunk_length_in_kb' : 128 };",
                keyspace);

        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("new_compression_options");

        // then
        assertThat(table.getOptions().getCompression())
                .contains(entry("class", "org.apache.cassandra.io.compress.DeflateCompressor"))
                .contains(entry("chunk_length_in_kb", "128"));
    }

    @Test(groups = "short")
    public void should_escape_single_quote_table_comment() {
        // given
        String cql = String.format("CREATE TABLE %s.single_quote (\n"
                        + "    c1 int PRIMARY KEY\n"
                        + ") WITH  comment = 'comment with single quote '' should work'",
                keyspace);
        // when
        session().execute(cql);
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("single_quote");
        // then
        assertThat(table.getOptions().getComment()).isEqualTo("comment with single quote ' should work");
        assertThat(table.asCQLQuery()).contains("comment = 'comment with single quote '' should work'");
    }

    @Test(groups = "short")
    public void should_not_mix_indexes_from_different_tables() {
        String[] statements = {
                "CREATE TABLE test_ab (a int PRIMARY KEY, b int);",
                "CREATE INDEX test_b on test_ab (b);",
                "CREATE TABLE test_cd (c int PRIMARY KEY, d int);",
                "CREATE INDEX test_d on test_cd (d);",
        };
        for (String statement : statements)
            session().execute(statement);

        TableMetadata table_ab = cluster().getMetadata().getKeyspace(keyspace).getTable("test_ab");
        TableMetadata table_cd = cluster().getMetadata().getKeyspace(keyspace).getTable("test_cd");

        assertThat(table_ab.getIndexes().size()).isEqualTo(1);
        assertThat(table_ab.getIndexes()).extracting("name").containsOnly("test_b");
        assertThat(table_cd.getIndexes().size()).isEqualTo(1);
        assertThat(table_cd.getIndexes()).extracting("name").containsOnly("test_d");
    }

    /**
     * Validates that the 'extensions' option is properly parsed when set on a table.
     * This value is currently not modifiable via CQL so we fake out a table containing
     * populated extensions by updating the extensions column in system_schema.tables
     * and forcing a schema refresh on it.
     *
     * @jira_ticket JAVA-938, CASSANDRA-9426
     * @test_category metadata
     */
    @Test(groups = "short")
    @CassandraVersion("3.0")
    public void should_parse_extensions_from_table_options() throws Exception {
        // given
        // create a simple table and retrieve it's metadata from system_schema.tables.
        String cql = String.format("CREATE TABLE %s.table_with_extensions (\n"
                + "    k text,\n"
                + "    c int,\n"
                + "    v timeuuid,\n"
                + "    PRIMARY KEY (k, c)\n"
                + ");", keyspace);
        session().execute(cql);

        // Manually change column value in system_schema.tables and force a schema refresh on that table.
        ImmutableMap<String, ByteBuffer> extensions = ImmutableMap.of("Hello", ByteBuffer.wrap("World".getBytes("UTF-8")));
        session().execute("update system_schema.tables set extensions=? where keyspace_name=? and table_name=?", extensions, keyspace, "table_with_extensions");
        cluster().manager.controlConnection.refreshSchema(SchemaElement.TABLE, keyspace, "table_with_extensions", null);

        // when retrieving the table's metadata.
        TableMetadata table = cluster().getMetadata().getKeyspace(keyspace).getTable("table_with_extensions");
        // then the table's options should contain populated extensions.
        assertThat(table.getOptions().getExtensions()).isEqualTo(extensions);
    }

    /**
     * Validates that a table with case-sensitive column names and column names
     * consisting of (quoted) reserved keywords is correctly parsed
     * and that the generated CQL is valid.
     *
     * @jira_ticket JAVA-1064
     * @test_category metadata
     */
    @Test(groups = "short")
    public void should_parse_table_with_case_sensitive_column_names_and_reserved_keywords() throws Exception {
        // given
        String c1 = Metadata.quote("quotes go \"\" here \"\" ");
        String c2 = Metadata.quote("\\x00\\x25");
        String c3 = Metadata.quote("columnfamily");
        String c4 = Metadata.quote("select");
        String c5 = Metadata.quote("who''s there'? ");
        String c6 = Metadata.quote("faux )");
        String c7 = Metadata.quote("COMPACT STORAGE");
        // single partition key
        String cql1 = String.format("CREATE TABLE %s.\"MyTable1\" ("
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "PRIMARY KEY (%s, %s, %s, %s, %s, %s)"
                + ")", keyspace, c1, c2, c3, c4, c5, c6, c7, c1, c2, c3, c4, c5, c6);
        // composite partition key
        String cql2 = String.format("CREATE TABLE %s.\"MyTable2\" ("
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "%s text, "
                + "PRIMARY KEY ((%s, %s), %s, %s, %s, %s)"
                + ")", keyspace, c1, c2, c3, c4, c5, c6, c7, c1, c2, c3, c4, c5, c6);
        // when
        execute(cql1, cql2);
        TableMetadata table1 = cluster().getMetadata().getKeyspace(keyspace).getTable("\"MyTable1\"");
        TableMetadata table2 = cluster().getMetadata().getKeyspace(keyspace).getTable("\"MyTable2\"");
        // then
        assertThat(table1)
                .hasColumn(c1)
                .hasColumn(c2)
                .hasColumn(c3)
                .hasColumn(c4)
                .hasColumn(c5)
                .hasColumn(c6)
                .hasColumn(c7);
        assertThat(table1.asCQLQuery()).startsWith(cql1);
        assertThat(table2)
                .hasColumn(c1)
                .hasColumn(c2)
                .hasColumn(c3)
                .hasColumn(c4)
                .hasColumn(c5)
                .hasColumn(c6)
                .hasColumn(c7);
        assertThat(table2.asCQLQuery()).startsWith(cql2);
        execute(
                "DROP TABLE \"MyTable1\"",
                "DROP TABLE \"MyTable2\"",
                table1.asCQLQuery(),
                table2.asCQLQuery()
        );
    }

}
