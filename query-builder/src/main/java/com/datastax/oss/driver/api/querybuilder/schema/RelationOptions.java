/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.querybuilder.schema;

import static com.datastax.oss.driver.api.querybuilder.SchemaBuilder.RowsPerPartition;

import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.compaction.CompactionStrategy;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface RelationOptions<SelfT extends RelationOptions<SelfT>>
    extends OptionProvider<SelfT> {

  /**
   * Defines the false-positive probability for SSTable bloom filters.
   *
   * <p>If no call was made to this method, the default value set is:
   *
   * <ul>
   *   <li><strong>0.01</strong> for the size-tiered compaction strategy;
   *   <li><strong>0.1</strong> for the leveled compaction strategy.
   * </ul>
   */
  @NonNull
  @CheckReturnValue
  default SelfT withBloomFilterFpChance(double bloomFilterFpChance) {
    return withOption("bloom_filter_fp_chance", bloomFilterFpChance);
  }

  /**
   * Defines whether or not change data capture is enabled.
   *
   * <p>Note that using this option with a version of Apache Cassandra less than 3.8 or DataStax
   * Enterprise 5.0 will raise a syntax error.
   *
   * <p>If no call is made to this method, the default value set is {@code false}.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withCDC(boolean enabled) {
    return withOption("cdc", enabled);
  }

  /**
   * Defines the caching criteria.
   *
   * <p>If no call is made to this method, the default value is determined by the global caching
   * properties in cassandra.yaml.
   *
   * @param keys If true, caches all keys, otherwise none.
   * @param rowsPerPartition Whether to cache ALL, NONE or the first N rows per partition.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withCaching(boolean keys, @NonNull RowsPerPartition rowsPerPartition) {
    return withOption(
        "caching",
        ImmutableMap.of(
            "keys", keys ? "ALL" : "NONE", "rows_per_partition", rowsPerPartition.getValue()));
  }

  /** Defines documentation for this relation. */
  @NonNull
  @CheckReturnValue
  default SelfT withComment(@NonNull String comment) {
    return withOption("comment", comment);
  }

  /**
   * Defines the compaction strategy to use.
   *
   * @see SchemaBuilder#sizeTieredCompactionStrategy()
   * @see SchemaBuilder#leveledCompactionStrategy()
   * @see SchemaBuilder#timeWindowCompactionStrategy()
   */
  @NonNull
  @CheckReturnValue
  default SelfT withCompaction(@NonNull CompactionStrategy<?> compactionStrategy) {
    return withOption("compaction", compactionStrategy.getOptions());
  }

  /**
   * Configures compression using the LZ4 algorithm with the given chunk length and crc check
   * chance.
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withLZ4Compression(int chunkLengthKB, double crcCheckChance) {
    return withCompression("LZ4Compressor", chunkLengthKB, crcCheckChance);
  }

  /**
   * Configures compression using the LZ4 algorithm using the default configuration (64kb
   * chunk_length, and 1.0 crc_check_chance).
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withLZ4Compression() {
    return withCompression("LZ4Compressor");
  }

  /**
   * Configures compression using the Snappy algorithm with the given chunk length and crc check
   * chance.
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withSnappyCompression(int chunkLengthKB, double crcCheckChance) {
    return withCompression("SnappyCompressor", chunkLengthKB, crcCheckChance);
  }

  /**
   * Configures compression using the Snappy algorithm using the default configuration (64kb
   * chunk_length, and 1.0 crc_check_chance).
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withSnappyCompression() {
    return withCompression("SnappyCompressor");
  }

  /**
   * Configures compression using the Deflate algorithm with the given chunk length and crc check
   * chance.
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withDeflateCompression(int chunkLengthKB, double crcCheckChance) {
    return withCompression("DeflateCompressor", chunkLengthKB, crcCheckChance);
  }

  /**
   * Configures compression using the Deflate algorithm using the default configuration (64kb
   * chunk_length, and 1.0 crc_check_chance).
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withDeflateCompression() {
    return withCompression("DeflateCompressor");
  }

  /**
   * Configures compression using the given algorithm using the default configuration (64kb
   * chunk_length, and 1.0 crc_check_chance).
   *
   * <p>Unless specifying a custom compression algorithm implementation, it is recommended to use
   * {@link #withLZ4Compression()}, {@link #withSnappyCompression()}, or {@link
   * #withDeflateCompression()}.
   *
   * @see #withCompression(String, int, double)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withCompression(@NonNull String compressionAlgorithmName) {
    return withOption("compression", ImmutableMap.of("class", compressionAlgorithmName));
  }

  /**
   * Configures compression using the given algorithm, chunk length and crc check chance.
   *
   * <p>Unless specifying a custom compression algorithm implementation, it is recommended to use
   * {@link #withLZ4Compression()}, {@link #withSnappyCompression()}, or {@link
   * #withDeflateCompression()}.
   *
   * @param compressionAlgorithmName The class name of the compression algorithm.
   * @param chunkLengthKB The chunk length in KB of compression blocks. Defaults to 64.
   * @param crcCheckChance The probability (0.0 to 1.0) that checksum will be checked on each read.
   *     Defaults to 1.0.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withCompression(
      @NonNull String compressionAlgorithmName, int chunkLengthKB, double crcCheckChance) {
    return withOption(
        "compression",
        ImmutableMap.of(
            "class",
            compressionAlgorithmName,
            "chunk_length_kb",
            chunkLengthKB,
            "crc_check_chance",
            crcCheckChance));
  }

  /** Defines that compression should be disabled. */
  @NonNull
  @CheckReturnValue
  default SelfT withNoCompression() {
    return withOption("compression", ImmutableMap.of("sstable_compression", ""));
  }

  /**
   * Defines the probability of read repairs being invoked over all replicas in the current data
   * center.
   *
   * <p>If no call is made to this method, the default value set is 0.0.
   *
   * @param dcLocalReadRepairChance the probability.
   * @return this {@code TableOptions} object.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withDcLocalReadRepairChance(double dcLocalReadRepairChance) {
    return withOption("dclocal_read_repair_chance", dcLocalReadRepairChance);
  }

  /**
   * Defines the default 'time to live' (expiration time) of writes in seconds.
   *
   * <p>If no call is made to this method, the default value is 0 (no TTL).
   */
  @NonNull
  @CheckReturnValue
  default SelfT withDefaultTimeToLiveSeconds(int ttl) {
    return withOption("default_time_to_live", ttl);
  }

  /**
   * Defines the time to wait before garbage collecting tombstones (deletion markers).
   *
   * <p>The default value allows a great deal of time for consistency to be achieved prior to
   * deletion. In many deployments this interval can be reduced, and in a single-node cluster it can
   * be safely set to zero.
   *
   * <p>If no call is made to this method, the default value set is 864000 secs (10 days).
   */
  @NonNull
  @CheckReturnValue
  default SelfT withGcGraceSeconds(int gcGraceSeconds) {
    return withOption("gc_grace_seconds", gcGraceSeconds);
  }

  /**
   * Defines the memtable flush period in milliseconds.
   *
   * <p>If set, this forces flushing of memtables after the specified time elapses.
   *
   * <p>If no call is made to this method, the default value is 0 (unset).
   */
  @NonNull
  @CheckReturnValue
  default SelfT withMemtableFlushPeriodInMs(int memtableFlushPeriodInMs) {
    return withOption("memtable_flush_period_in_ms", memtableFlushPeriodInMs);
  }

  /**
   * Defines the minimum index interval. This is the gap between index entries in the index summary.
   * A lower value will increase the size of the index (more RAM usage) but potentially improve disk
   * I/O.
   *
   * <p>If no call is made to this method, the default value set is 128.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withMinIndexInterval(int min) {
    return withOption("min_index_interval", min);
  }

  /**
   * Defines the maximum index interval.
   *
   * <p>If no call is made to this method, the default value set is 2048.
   *
   * @see #withMinIndexInterval(int)
   */
  @NonNull
  @CheckReturnValue
  default SelfT withMaxIndexInterval(int max) {
    return withOption("max_index_interval", max);
  }

  /**
   * Defines the probability with which read repairs should be invoked on non-quorum reads. The
   * value must be between 0 and 1.
   *
   * <p>If no call is made to this method, the default value set is 0.1.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withReadRepairChance(double readRepairChance) {
    return withOption("read_repair_chance", readRepairChance);
  }

  /**
   * Defines the configuration for coordinator to replica speculative retries.
   *
   * <p>This overrides the normal read timeout when read_repair_chance is not 1.0, sending a request
   * to other replica(s) to service reads.
   *
   * <p>Valid values include:
   *
   * <ul>
   *   <li>ALWAYS: Retry reads of all replicas.
   *   <li>Xpercentile: Retry reads based on the effect on throughput and latency.
   *   <li>Yms: Retry reads after specified milliseconds.
   *   <li>NONE: Do not retry reads.
   * </ul>
   *
   * <p>Using the speculative retry property, you can configure rapid read protection in Cassandra
   * 2.0.2 and later. Use this property to retry a request after some milliseconds have passed or
   * after a percentile of the typical read latency has been reached, which is tracked per table.
   *
   * <p>If no call is made to this method, the default value set is {@code 99percentile}.
   */
  @NonNull
  @CheckReturnValue
  default SelfT withSpeculativeRetry(@NonNull String speculativeRetry) {
    return withOption("speculative_retry", speculativeRetry);
  }
}
