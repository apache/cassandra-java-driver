/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
package com.datastax.driver.core.schemabuilder;

import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;

/**
 * Table options
 * <p>
 *     This class is abstract and not meant to use directly.
 *     <br/>
 *     Concrete implementations are {@link com.datastax.driver.core.schemabuilder.Create.Options} and {@link com.datastax.driver.core.schemabuilder.Alter.Options}
 * </p>
 * <p>
 *     The &lt;T&gt; parameter type is here to allow the usage of <strong>covariant return type</strong> and makes the builder pattern work for different sub-classes
 * </p>
 * <p>
 *     @see <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/tabProp.html" target="_blank">details on table options</a>
 * </p>
 * @param <T> the concrete sub-class of {@link com.datastax.driver.core.schemabuilder.TableOptions}
 */
public abstract class TableOptions<T extends TableOptions> {

    static final String VALUE_SEPARATOR = " : ";
    static final String START_SUB_OPTIONS = "{";
    static final String SUB_OPTION_SEPARATOR = ", ";
    static final String END_SUB_OPTIONS = "}";
    static final String OPTION_ASSIGNMENT = " = ";
    static final String QUOTE = "'";
    static final String OPTION_SEPARATOR = " AND ";


    private SchemaStatement schemaStatement;

    private Optional<Caching> caching = Optional.absent();
    private Optional<CachingRowsPerPartition> cachingRowsPerPartition = Optional.absent();

    private Optional<Double> bloomFilterFPChance = Optional.absent();

    private Optional<String> comment = Optional.absent();

    private Optional<CompressionOptions> compressionOptions = Optional.absent();

    private Optional<CompactionOptions> compactionOptions = Optional.absent();

    private Optional<Double> dcLocalReadRepairChance = Optional.absent();

    private Optional<Integer> defaultTTL = Optional.absent();

    private Optional<Long> gcGraceSeconds = Optional.absent();

    private Optional<Integer> minIndexInterval = Optional.absent();
    private Optional<Integer> maxIndexInterval = Optional.absent();

    private Optional<Long> memtableFlushPeriodInMillis = Optional.absent();

    private Optional<Boolean> populateIOOnCacheFlush = Optional.absent();

    private Optional<Double> readRepairChance = Optional.absent();

    private Optional<Boolean> replicateOnWrite = Optional.absent();

    private Optional<SpeculativeRetryValue> speculativeRetry = Optional.absent();

    private List<RawOption> customOptions = new ArrayList<RawOption>();

    TableOptions(SchemaStatement schemaStatement) {
        this.schemaStatement = schemaStatement;
    }

    /**
     * Define the caching type for Cassandra 2.0.x
     * <p>
     *     Default caching type = KEYS_ONLY
     * </p>
     * @param caching caching type. Available values are NONE, ALL, KEYS_ONLY & ROWS_ONLY
     * @return this table options
     */
    public T caching(Caching caching) {
        this.caching = Optional.fromNullable(caching);
        return (T) this;
    }

    /**
     * Define the number of rows to be cached per partition when Row Caching is enabled. This feature is only applicable to Cassandra 2.1.x
     * <ul>
     *     <li>NONE: Do not cache rows.</li>
     *     <li>ALL: cache all rows for a given partition.<strong>Be careful when choosing this option, you can starve quickly Cassandra memory if your partition is very large</strong></li>
     *     <li>int: define a number of rows to cache.</li>
     * </ul>
     *
     * Use the static methods on {@link com.datastax.driver.core.schemabuilder.TableOptions.CachingRowsPerPartition} to build the option
     * <p>
     *     Default caching type = ALL
     *     Default rows_per_partition = NONE
     * </p>
     * @param caching caching type. Available values are NONE, ALL
     * @param rowsPerPartition number of rows to cache per partition. Possible values are NONE, ALL or an integer number
     * @return this table options
     */
    public T caching(Caching caching, CachingRowsPerPartition rowsPerPartition) {
        this.caching = Optional.fromNullable(caching);
        this.cachingRowsPerPartition = Optional.fromNullable(rowsPerPartition);
        return (T) this;
    }

    /**
     * Desired false-positive probability for SSTable Bloom filters
     * <p>
     *     If not set, for SizeTiered strategy, default = <strong>0.01</strong>, for Leveled strategy, default = <strong>0.1</strong>
     * </p>
     * @param fpChance the false positive change. This value should be between 0 and 1.0
     * @return this table options
     */
    public T bloomFilterFPChance(Double fpChance) {
        validateRateValue(fpChance, "Bloom filter false positive change");
        this.bloomFilterFPChance = Optional.fromNullable(fpChance);
        return (T) this;
    }

    /**
     * A human readable comment describing the table
     * @param comment comment for the table
     * @return this table options
     */
    public T comment(String comment) {
        this.comment = Optional.fromNullable(comment);
        return (T) this;
    }

    /**
     * Define the compression options
     * @param compressionOptions the compression options.
     *        Use the {@link com.datastax.driver.core.schemabuilder.TableOptions.CompressionOptions} to build an instance of compression options
     * @return this table options
     */
    public T compressionOptions(CompressionOptions compressionOptions) {
        this.compressionOptions = Optional.fromNullable(compressionOptions);
        return (T) this;
    }

    /**
     * Define the compaction options
     * @param compactionOptions the compaction options.
     *        Use the {@link TableOptions.CompactionOptions} to build an instance of compaction options
     * @return this table options
     */
    public T compactionOptions(CompactionOptions compactionOptions) {
        this.compactionOptions = Optional.fromNullable(compactionOptions);
        return (T) this;
    }

    /**
     * Specifies the probability of read repairs being invoked over all replicas in the current data center
     * <p>
     *     If not set, default to 0.0
     * </p>
     * @param dcLocalReadRepairChance local Data Center read repair change
     * @return this table options
     */
    public T dcLocalReadRepairChance(Double dcLocalReadRepairChance) {
        validateRateValue(dcLocalReadRepairChance, "DC local read repair chance");
        this.dcLocalReadRepairChance = Optional.fromNullable(dcLocalReadRepairChance);
        return (T) this;
    }


    /**
     * The default expiration time in seconds for a table. Used in MapReduce/Hive scenarios when you have no control of TTL
     * <p>
     *     If not set, default =0
     * </p>
     * @param defaultTimeToLive default time to live in seconds for a table
     * @return this table options
     */
    public T defaultTimeToLive(Integer defaultTimeToLive) {
        this.defaultTTL = Optional.fromNullable(defaultTimeToLive);
        return (T) this;
    }

    /**
     * Specifies the time to wait before garbage collecting tombstones (deletion markers).
     * The default value allows a great deal of time for consistency to be achieved prior to deletion.
     * In many deployments this interval can be reduced, and in a single-node cluster it can be safely set to zero
     * <p>
     *     If not set, default = 864000 secs (10 days)
     * </p>
     * @param gcGraceSeconds GC grace seconds
     * @return this table options
     */
    public T gcGraceSeconds(Long gcGraceSeconds) {
        this.gcGraceSeconds = Optional.fromNullable(gcGraceSeconds);
        return (T) this;
    }

    /**
     * The min_index_interval and max_index_interval (Cassandra 2.1) properties control the sampling of entries from the primary row index,
     * configure sample frequency of the partition summary by changing the index interval.
     * After changing the index interval, SSTables are written to disk with new information.
     *
     * The interval corresponds to the number of index entries that are skipped between taking each sample.
     * By default Cassandra samples one row key out of every 128.
     * The larger the interval, the smaller and less effective the sampling.
     * The larger the sampling, the more effective the index, but with increased memory usage.
     * In Cassandra 2.0.x, generally, the best trade off between memory usage and performance is a value between
     * 128 and 512 in combination with a large table key cache.
     * However, if you have small rows (many to an OS page), you may want to increase the sample size,
     * which often lowers memory usage without an impact on performance.
     * For large rows, decreasing the sample size may improve read performance.
     * <p>
     *     If not set, default = 128
     * </p>
     * @param minIndexInterval index interval
     * @return this table options
     */
    public T minIndexInterval(Integer minIndexInterval) {
        this.minIndexInterval = Optional.fromNullable(minIndexInterval);
        return (T) this;
    }

    /**
     * The min_index_interval and max_index_interval (Cassandra 2.1) properties control the sampling of entries from the primary row index,
     * configure sample frequency of the partition summary by changing the index interval.
     * After changing the index interval, SSTables are written to disk with new information.
     *
     * The interval corresponds to the number of index entries that are skipped between taking each sample.
     * By default Cassandra samples one row key out of every 128.
     * The larger the interval, the smaller and less effective the sampling.
     * The larger the sampling, the more effective the index, but with increased memory usage.
     * In Cassandra 2.0.x, generally, the best trade off between memory usage and performance is a value between
     * 128 and 512 in combination with a large table key cache.
     * However, if you have small rows (many to an OS page), you may want to increase the sample size,
     * which often lowers memory usage without an impact on performance.
     * For large rows, decreasing the sample size may improve read performance.
     * <p>
     *     If not set, default = 2048
     * </p>
     * @param maxIndexInterval index interval
     * @return this table options
     */
    public T maxIndexInterval(Integer maxIndexInterval) {
        this.maxIndexInterval = Optional.fromNullable(maxIndexInterval);
        return (T) this;
    }

    /**
     * Forces flushing of the memtable after the specified time in milliseconds elapses
     * <p>
     *     If not set, default = 0
     * </p>
     * @param memtableFlushPeriodInMillis memtable flush period in milli seconds
     * @return this table options
     */
    public T memtableFlushPeriodInMillis(Long memtableFlushPeriodInMillis) {
        this.memtableFlushPeriodInMillis = Optional.fromNullable(memtableFlushPeriodInMillis);
        return (T) this;
    }

    /**
     * Adds newly flushed or compacted sstables to the operating system page cache, potentially evicting other cached data to make room.
     * Enable when all data in the table is expected to fit in memory.
     *
     * See also the global option <a href="http://www.datastax.com/documentation/cassandra/2.0/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__compaction_preheat_key_cache">compaction_preheat_key_cache</a>
     * @param populateIOOnCacheFlush whether populate IO cache on flush of sstables
     * @return this table options
     */
    public T populateIOOnCacheFlush(Boolean populateIOOnCacheFlush) {
        this.populateIOOnCacheFlush = Optional.fromNullable(populateIOOnCacheFlush);
        return (T) this;
    }

    /**
     * Specifies the probability with which read repairs should be invoked on non-quorum reads. The value must be between 0 and 1.
     * <p>
     *     If not set, default = 0.1
     * </p>
     * @param readRepairChance read repair chance
     * @return this table options
     */
    public T readRepairChance(Double readRepairChance) {
        validateRateValue(readRepairChance, "Read repair chance");
        this.readRepairChance = Optional.fromNullable(readRepairChance);
        return (T) this;
    }


    /**
     * Applies only to counter tables.
     * When set to true, replicates writes to all affected replicas regardless of the consistency level specified by the client for a write request.
     * For counter tables, this should always be set to true
     * <p>
     *     If not set, default = true
     * </p>
     * @param replicateOnWrite whether replicate data on write
     * @return this table options
     */
    public T replicateOnWrite(Boolean replicateOnWrite) {
        this.replicateOnWrite = Optional.fromNullable(replicateOnWrite);
        return (T) this;
    }

    /**
     * To override normal read timeout when read_repair_chance is not 1.0, sending another request to read, choose one of these values and use the property to create
     * or alter the table:
     * <ul>
     *     <li>ALWAYS: Retry reads of all replicas.</li>
     *     <li>Xpercentile: Retry reads based on the effect on throughput and latency.</li>
     *     <li>Yms: Retry reads after specified milliseconds.</li>
     *     <li>NONE: Do not retry reads.</li>
     * </ul>
     *
     * Using the speculative retry property, you can configure rapid read protection in Cassandra 2.0.2.
     * Use this property to retry a request after some milliseconds have passed or after a percentile of the typical read latency has been reached,
     * which is tracked per table.
     *
     * <p>
     *     If not set, default = 99percentile Cassandra 2.0.2 and later
     * </p>
     * @param speculativeRetry the speculative retry.
     *      Use {@link TableOptions.SpeculativeRetryValue} class to build an instance
     * @return this table options
     */
    public T speculativeRetry(SpeculativeRetryValue speculativeRetry) {
        this.speculativeRetry = Optional.fromNullable(speculativeRetry);
        return (T) this;
    }

    /**
     * Input your own option as key/value pair of String.
     * <br/>
     * This method can be useful when new options have been added to Cassandra but the SchemaBuilder is not yet updated
     * @param key the name of the option
     * @param value its value in String representation
     * @return this table options
     */
    public T customOption(String key, String value) {
        if (Strings.isNullOrEmpty(key)) {
            throw new IllegalArgumentException("Key for custom option should not be null or blank");
        }

        if (Strings.isNullOrEmpty(value)) {
            throw new IllegalArgumentException("Value for custom option should not be null or blank");
        }
        customOptions.add(new RawOption(key, value));
        return (T) this;
    }

    List<String> buildCommonOptions() {
        List<String> options = new ArrayList<String>();

        buildCachingOptions(options);

        if (bloomFilterFPChance.isPresent()) {
            options.add(new StringBuilder("bloom_filter_fp_chance").append(OPTION_ASSIGNMENT).append(bloomFilterFPChance.get()).toString());
        }

        if (comment.isPresent()) {
            options.add(new StringBuilder("comment").append(OPTION_ASSIGNMENT).append(QUOTE).append(comment.get()).append(QUOTE).toString());
        }

        if (compressionOptions.isPresent()) {
            options.add(new StringBuilder("compression").append(OPTION_ASSIGNMENT).append(compressionOptions.get().build()).toString());
        }

        if (compactionOptions.isPresent()) {
            options.add(new StringBuilder("compaction").append(OPTION_ASSIGNMENT).append(compactionOptions.get().build()).toString());
        }

        if (dcLocalReadRepairChance.isPresent()) {
            options.add(new StringBuilder("dclocal_read_repair_chance").append(OPTION_ASSIGNMENT).append(dcLocalReadRepairChance.get()).toString());
        }

        if (defaultTTL.isPresent()) {
            options.add(new StringBuilder("default_time_to_live").append(OPTION_ASSIGNMENT).append(defaultTTL.get()).toString());
        }

        if (gcGraceSeconds.isPresent()) {
            options.add(new StringBuilder("gc_grace_seconds").append(OPTION_ASSIGNMENT).append(gcGraceSeconds.get()).toString());
        }

        if (minIndexInterval.isPresent()) {
            options.add(new StringBuilder("min_index_interval").append(OPTION_ASSIGNMENT).append(minIndexInterval.get()).toString());
        }

        if (maxIndexInterval.isPresent()) {
            options.add(new StringBuilder("max_index_interval").append(OPTION_ASSIGNMENT).append(maxIndexInterval.get()).toString());
        }

        if (memtableFlushPeriodInMillis.isPresent()) {
            options.add(new StringBuilder("memtable_flush_period_in_ms").append(OPTION_ASSIGNMENT).append(memtableFlushPeriodInMillis.get()).toString());
        }

        if (populateIOOnCacheFlush.isPresent()) {
            options.add(new StringBuilder("populate_io_cache_on_flush").append(OPTION_ASSIGNMENT).append(populateIOOnCacheFlush.get()).toString());
        }

        if (readRepairChance.isPresent()) {
            options.add(new StringBuilder("read_repair_chance").append(OPTION_ASSIGNMENT).append(readRepairChance.get()).toString());
        }

        if (replicateOnWrite.isPresent()) {
            options.add(new StringBuilder("replicate_on_write").append(OPTION_ASSIGNMENT).append(replicateOnWrite.get()).toString());
        }

        if (speculativeRetry.isPresent()) {
            options.add(new StringBuilder("speculative_retry").append(OPTION_ASSIGNMENT).append(speculativeRetry.get().value()).toString());
        }

        if (!customOptions.isEmpty()) {
            for (RawOption customOption : customOptions) {
                options.add(new StringBuilder(customOption.key).append(OPTION_ASSIGNMENT).append(customOption.value).toString());
            }
        }
        return options;
    }

    private void buildCachingOptions(List<String> options) {
        if (caching.isPresent()) {
            if (cachingRowsPerPartition.isPresent()) {
                final Caching cachingType = caching.get();
                if ((cachingType == Caching.ALL || cachingType == Caching.NONE)) {
                    options.add(new StringBuilder("caching")
                            .append(OPTION_ASSIGNMENT)
                            .append(START_SUB_OPTIONS)
                            .append(QUOTE).append("keys").append(QUOTE)
                            .append(VALUE_SEPARATOR)
                            .append(caching.get().value())
                            .append(SUB_OPTION_SEPARATOR)
                            .append(QUOTE).append("rows_per_partition").append(QUOTE)
                            .append(VALUE_SEPARATOR)
                            .append(cachingRowsPerPartition.get().value())
                            .append(END_SUB_OPTIONS)
                            .toString());
                } else {
                    throw new IllegalStateException("Cannot use caching type : "+cachingType.name()+" with the option 'rows_per_partition'. Please use ALL or NONE as caching type ");
                }
            } else {
                options.add(new StringBuilder("caching").append(OPTION_ASSIGNMENT).append(caching.get()).toString());
            }
        }
    }

    abstract String buildOptions();

    String build() {
        return schemaStatement.buildInternal();
    }

    static void validateRateValue(Double rateValue, String property) {
        if (rateValue != null && (rateValue < 0 || rateValue > 1.0)) {
            throw new IllegalArgumentException(property + " should be between 0 and 1");
        }
    }

    /**
     * Define table caching.
     * <p>
     *     Possible values are NONE, ALL, KEYS_ONLY & ROWS_ONLY
     * </p>
     *
     */
    public static enum Caching {
        ALL("'all'"), KEYS_ONLY("'keys_only'"), ROWS_ONLY("'rows_only'"), NONE("'none'");

        private String value;

        Caching(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    /**
     * Compaction options
     * <p>
     *      This is an abstract class. Concrete classes are {@link TableOptions.CompactionOptions.SizeTieredCompactionStrategyOptions}
     *      and {@link TableOptions.CompactionOptions.LeveledCompactionStrategyOptions}
     * </p>
     * <p>
     *      The parameter type &lt;T&gt; allows the usage of <strong>covariant return type</strong> to make the builder work
     * </p>
     * <p>
     *     @see <a href="http://www.datastax.com/documentation/cql/3.1/cql/cql_reference/compactSubprop.html" target="_blank">details on sub-properties of compaction</a>
     * </p>
     * </p>
     * @param <T> the type of the sub-class
     */
    public static abstract class CompactionOptions<T extends CompactionOptions> {

        private Strategy strategy;

        private Optional<Double> bucketHigh = Optional.absent();

        private Optional<Double> bucketLow = Optional.absent();

        private Optional<Double> coldReadsRatioToOmit = Optional.absent();

        private Optional<Boolean> enableAutoCompaction = Optional.absent();

        private Optional<Integer> minThreshold = Optional.absent();

        private Optional<Integer> maxThreshold = Optional.absent();

        private Optional<Long> minSSTableSizeInBytes = Optional.absent();

        private Optional<Integer> ssTableSizeInMB = Optional.absent();

        private Optional<Integer> tombstoneCompactionIntervalInDay = Optional.absent();

        private Optional<Double> tombstoneThreshold = Optional.absent();


        private CompactionOptions(Strategy compactionStrategy) {
            this.strategy = compactionStrategy;
        }

        /**
         * Compaction options for SizeTiered strategy
         * @return a {@link TableOptions.CompactionOptions.SizeTieredCompactionStrategyOptions} instance
         */
        public static SizeTieredCompactionStrategyOptions sizedTieredStategy() {
            return new SizeTieredCompactionStrategyOptions();
        }

        /**
         * Compaction options for Leveled strategy
         * @return a {@link TableOptions.CompactionOptions.LeveledCompactionStrategyOptions} instance
         */
        public static LeveledCompactionStrategyOptions leveledStrategy() {
            return new LeveledCompactionStrategyOptions();
        }

        /**
         * Enables or disables background compaction
         * <p>
         *     If not set, default = true
         * </p>
         * @param enableAutoCompaction whether to enable auto compaction for the table
         * @return this compaction options
         */
        public T enableAutoCompaction(Boolean enableAutoCompaction) {
            this.enableAutoCompaction = Optional.fromNullable(enableAutoCompaction);
            return (T) this;
        }

        /**
         * In SizeTieredCompactionStrategy, sets the maximum number of SSTables to allow in a minor compaction.
         * In LeveledCompactionStrategy (LCS), it applies to L0 when L0 gets behind, that is, when L0 accumulates more than MAX_COMPACTING_L0 SSTables.
         * <p>
         *     If not set, default = 32
         * </p>
         * @param maxThreshold max threshold
         * @return this compaction options
         */
        public T maxThreshold(Integer maxThreshold) {
            this.maxThreshold = Optional.fromNullable(maxThreshold);
            return (T) this;
        }

        /**
         * The minimum number of days to wait after an SSTable creation time before considering the SSTable for tombstone compaction.
         * Tombstone compaction is the compaction triggered if the SSTable has more garbage-collectable tombstones than tombstone_threshold.
         * <p>
         *     If not set, default = 1 (day)
         * </p>
         * @param tombstoneCompactionInterval tombstone compaction interval in day.
         * @return this compaction options
         */
        public T tombstoneCompactionIntervalInDay(Integer tombstoneCompactionInterval) {
            this.tombstoneCompactionIntervalInDay = Optional.fromNullable(tombstoneCompactionInterval);
            return (T) this;
        }

        /**
         * A ratio of garbage-collectable tombstones to all contained columns,
         * which if exceeded by the SSTable triggers compaction (with no other SSTables) for the purpose of purging the tombstones
         * <p>
         *     If not set, default = 0.2
         * </p>
         * @param tombstoneThreshold tombstone compaction interval in day.
         * @return this compaction options
         */
        public T tombstoneThreshold(Double tombstoneThreshold) {
            validateRateValue(tombstoneThreshold, "Tombstone threshold");
            this.tombstoneThreshold = Optional.fromNullable(tombstoneThreshold);
            return (T) this;
        }


        List<String> buildCommonOptions() {

            List<String> options = new ArrayList<String>();
            options.add(new StringBuilder("'class'").append(VALUE_SEPARATOR).append(strategy.strategyClass()).toString());

            if (enableAutoCompaction.isPresent()) {
                options.add(new StringBuilder("'enabled'").append(VALUE_SEPARATOR).append(enableAutoCompaction.get()).toString());
            }

            if (maxThreshold.isPresent()) {
                options.add(new StringBuilder("'max_threshold'").append(VALUE_SEPARATOR).append(maxThreshold.get()).toString());
            }

            if (tombstoneCompactionIntervalInDay.isPresent()) {
                options.add(new StringBuilder("'tombstone_compaction_interval'").append(VALUE_SEPARATOR).append(tombstoneCompactionIntervalInDay.get()).toString());
            }

            if (tombstoneThreshold.isPresent()) {
                options.add(new StringBuilder("'tombstone_threshold'").append(VALUE_SEPARATOR).append(tombstoneThreshold.get()).toString());
            }

            return options;
        }

        public abstract String build();

        /**
         * Compaction options specific to SizeTiered strategy
         */
        public static class SizeTieredCompactionStrategyOptions extends CompactionOptions<SizeTieredCompactionStrategyOptions> {

            private SizeTieredCompactionStrategyOptions() {
                super(Strategy.SIZED_TIERED);
            }

            /**
             * Size-tiered compaction strategy (STCS) considers SSTables to be within the same bucket if the SSTable size diverges by 50%
             * or less from the default bucket_low and default bucket_high values: [average-size × bucket_low, average-size × bucket_high].
             * <p>
             *     If not set, default = 1.5
             * </p>
             * @param bucketHigh bucket high
             * @return
             */
            public SizeTieredCompactionStrategyOptions bucketHigh(Double bucketHigh) {
                super.bucketHigh = Optional.fromNullable(bucketHigh);
                return this;
            }

            /**
             * Size-tiered compaction strategy (STCS) considers SSTables to be within the same bucket if the SSTable size diverges by 50%
             * or less from the default bucket_low and default bucket_high values: [average-size × bucket_low, average-size × bucket_high].
             * <p>
             *     If not set, default = 0.5
             * </p>
             * @param bucketLow bucket low
             * @return
             */
            public SizeTieredCompactionStrategyOptions bucketLow(Double bucketLow) {
                super.bucketLow = Optional.fromNullable(bucketLow);
                return this;
            }

            /**
             * The maximum percentage of reads/sec that ignored SSTables may account for.
             * The recommended range of values is 0.0 and 1.0.
             * In Cassandra 2.0.3 and later, you can enable the cold_reads_to_omit property to tune performace per table.
             * The <a href="http://www.datastax.com/dev/blog/optimizations-around-cold-sstables">Optimizations around Cold SSTables</a> blog includes detailed information tuning performance using this property,
             * which avoids compacting cold SSTables. Use the ALTER TABLE command to configure cold_reads_to_omit.
             * <p>
             *     If not set, default = 0.0 (disabled)
             * </p>
             * @param coldReadsRatio
             * @return
             */
            public SizeTieredCompactionStrategyOptions coldReadsRatioToOmit(Double coldReadsRatio) {
                validateRateValue(coldReadsRatio, "Cold read ratio to omit ");
                super.coldReadsRatioToOmit = Optional.fromNullable(coldReadsRatio);
                return this;
            }

            /**
             * In SizeTieredCompactionStrategy sets the minimum number of SSTables to trigger a minor compaction
             * <p>
             *     If not set, default = 4
             * </p>
             * @param minThreshold min threshold
             * @return
             */
            public SizeTieredCompactionStrategyOptions minThreshold(Integer minThreshold) {
                super.minThreshold = Optional.fromNullable(minThreshold);
                return this;
            }

            /**
             * The SizeTieredCompactionStrategy groups SSTables for compaction into buckets.
             * The bucketing process groups SSTables that differ in size by less than 50%. This results in a bucketing process that is too fine grained for small SSTables.
             * If your SSTables are small, use min_sstable_size to define a size threshold (in bytes) below which all SSTables belong to one unique bucket
             * <p>
             *     If not set, default = 52428800 (50Mb)
             * </p>
             * @param minSSTableSize min SSTable size in bytes
             * @return
             */
            public SizeTieredCompactionStrategyOptions minSSTableSizeInBytes(Long minSSTableSize) {
                super.minSSTableSizeInBytes = Optional.fromNullable(minSSTableSize);
                return this;
            }

            @Override
            public String build() {
                final List<String> generalOptions = super.buildCommonOptions();

                List<String> options = new ArrayList<String>(generalOptions);

                if (super.bucketHigh.isPresent()) {
                    options.add(new StringBuilder("'bucket_high'").append(VALUE_SEPARATOR).append(super.bucketHigh.get()).toString());
                }

                if (super.bucketLow.isPresent()) {
                    options.add(new StringBuilder("'bucket_low'").append(VALUE_SEPARATOR).append(super.bucketLow.get()).toString());
                }

                if (super.coldReadsRatioToOmit.isPresent()) {
                    options.add(new StringBuilder("'cold_reads_to_omit'").append(VALUE_SEPARATOR).append(super.coldReadsRatioToOmit.get()).toString());
                }

                if (super.minThreshold.isPresent()) {
                    options.add(new StringBuilder("'min_threshold'").append(VALUE_SEPARATOR).append(super.minThreshold.get()).toString());
                }

                if (super.minSSTableSizeInBytes.isPresent()) {
                    options.add(new StringBuilder("'min_sstable_size'").append(VALUE_SEPARATOR).append(super.minSSTableSizeInBytes.get()).toString());
                }
                return new StringBuilder(START_SUB_OPTIONS).append(Joiner.on(SUB_OPTION_SEPARATOR).join(options)).append(END_SUB_OPTIONS).toString();
            }
        }

        /**
         * Compaction options specific to Leveled strategy
         */
        public static class LeveledCompactionStrategyOptions extends CompactionOptions<LeveledCompactionStrategyOptions> {

            private LeveledCompactionStrategyOptions() {
                super(Strategy.LEVELED);
            }

            /**
             * The target size for SSTables that use the leveled compaction strategy.
             * Although SSTable sizes should be less or equal to sstable_size_in_mb, it is possible to have a larger SSTable during compaction.
             * This occurs when data for a given partition key is exceptionally large. The data is not split into two SSTables
             * <p>
             *     If not set, default = 160 Mb
             * </p>
             * @param ssTableSizeInMB  SSTable size in Mb
             * @return
             */
            public LeveledCompactionStrategyOptions ssTableSizeInMB(Integer ssTableSizeInMB) {
                super.ssTableSizeInMB = Optional.fromNullable(ssTableSizeInMB);
                return this;
            }

            @Override
            public String build() {
                final List<String> generalOptions = super.buildCommonOptions();

                List<String> options = new ArrayList<String>(generalOptions);

                if (super.ssTableSizeInMB.isPresent()) {
                    options.add(new StringBuilder("'sstable_size_in_mb'").append(VALUE_SEPARATOR).append(super.ssTableSizeInMB.get()).toString());
                }
                return new StringBuilder(START_SUB_OPTIONS).append(Joiner.on(SUB_OPTION_SEPARATOR).join(options)).append(END_SUB_OPTIONS).toString();
            }

        }

        /**
         * Compaction strategies. Possible values: SIZED_TIERED & LEVELED
         */
        public static enum Strategy {
            SIZED_TIERED("'SizeTieredCompactionStrategy'"), LEVELED("'LeveledCompactionStrategy'");

            private String strategyClass;

            Strategy(String strategyClass) {
                this.strategyClass = strategyClass;
            }

            public String strategyClass() {
                return strategyClass;
            }

            @Override
            public String toString() {
                return strategyClass;
            }
        }

    }

    /**
     * Compression options
     */
    public static class CompressionOptions {

        private Algorithm algorithm;

        private Optional<Integer> chunckLengthInKb = Optional.absent();

        private Optional<Double> crcCheckChance = Optional.absent();


        public CompressionOptions(Algorithm algorithm) {
            this.algorithm = algorithm;
        }

        /**
         * No compression
         * @return compression options
         */
        public static CompressionOptions none() {
            return new NoCompression();
        }

        /**
         * LZ4 compression
         * @return compression options
         */
        public static CompressionOptions lz4() {
            return new CompressionOptions(Algorithm.LZ4);
        }

        /**
         * Snappy compression
         * @return compression options
         */
        public static CompressionOptions snappy() {
            return new CompressionOptions(Algorithm.SNAPPY);
        }

        /**
         * Deflate compression
         * @return compression options
         */
        public static CompressionOptions deflate() {
            return new CompressionOptions(Algorithm.DEFLATE);
        }

        /**
         * On disk, SSTables are compressed by block to allow random reads.
         * This subproperty of compression defines the size (in KB) of the block.
         * Values larger than the default value might improve the compression rate, but increases the minimum size of data to be read from disk when a read occurs.
         * The default value is a good middle-ground for compressing tables.
         * Adjust compression size to account for read/write access patterns (how much data is typically requested at once) and the average size of rows in the table.
         * <p>
         *     If not set, default = 64kb
         * </p>
         * @param chunkLengthInKb chunk length in Kb
         * @return
         */
        public CompressionOptions withChunkLengthInKb(Integer chunkLengthInKb) {
            this.chunckLengthInKb = Optional.fromNullable(chunkLengthInKb);
            return this;
        }

        /**
         * When compression is enabled, each compressed block includes a checksum of that block for the purpose of detecting disk bitrate and avoiding the propagation
         * of corruption to other replica. This option defines the probability with which those checksums are checked during read.
         * By default they are always checked. Set to 0 to disable checksum checking and to 0.5, for instance, to check them on every other read.
         * <p>
         *     If not set, default = 1.0 (always check)
         * </p>
         * @param crcCheckChance CRC check chance
         * @return
         */
        public CompressionOptions withCRCCheckChance(Double crcCheckChance) {
            validateRateValue(crcCheckChance, "CRC check chance");
            this.crcCheckChance = Optional.fromNullable(crcCheckChance);
            return this;
        }

        public String build() {
            List<String> options = new ArrayList<String>();
            options.add(new StringBuilder("'sstable_compression'").append(VALUE_SEPARATOR).append(algorithm.value()).toString());

            if (chunckLengthInKb.isPresent()) {
                options.add(new StringBuilder("'chunk_length_kb'").append(VALUE_SEPARATOR).append(chunckLengthInKb.get()).toString());
            }

            if (crcCheckChance.isPresent()) {
                options.add(new StringBuilder("'crc_check_chance'").append(VALUE_SEPARATOR).append(crcCheckChance.get()).toString());
            }
            return new StringBuilder().append(START_SUB_OPTIONS).append(Joiner.on(SUB_OPTION_SEPARATOR).join(options)).append(END_SUB_OPTIONS).toString();
        }

        /**
         * Compression algorithms. Possible values: NONE, LZ4, SNAPPY, DEFLATE
         */
        public static enum Algorithm {
            NONE("''"), LZ4("'LZ4Compressor'"), SNAPPY("'SnappyCompressor'"), DEFLATE("'DeflateCompressor'");

            private String value;

            Algorithm(String value) {
                this.value = value;
            }

            public String value() {
                return value;
            }

            @Override
            public String toString() {
                return value;
            }
        }

        public static class NoCompression extends CompressionOptions {

            public NoCompression() {
                super(Algorithm.NONE);
            }

            public CompressionOptions withChunkLengthInKb(Integer chunkLengthInKb) {
                return this;
            }

            public CompressionOptions withCRCCheckChance(Double crcCheckChance) {
                return this;
            }
        }
    }

    /**
     * To override normal read timeout when read_repair_chance is not 1.0, sending another request to read, choose one of these values and use the property to create
     * or alter the table:
     * <ul>
     *     <li>ALWAYS: Retry reads of all replicas.</li>
     *     <li>Xpercentile: Retry reads based on the effect on throughput and latency.</li>
     *     <li>Yms: Retry reads after specified milliseconds.</li>
     *     <li>NONE: Do not retry reads.</li>
     * </ul>
     *
     * Using the speculative retry property, you can configure rapid read protection in Cassandra 2.0.2.
     * Use this property to retry a request after some milliseconds have passed or after a percentile of the typical read latency has been reached,
     * which is tracked per table.
     *
     * <p>
     *     If not set, default = 99percentile Cassandra 2.0.2 and later
     * </p>
     */
    public static class SpeculativeRetryValue {

        private String value;

        private SpeculativeRetryValue(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        /**
         * Deactivate speculative retry
         * @return speculative retry value
         */
        public static SpeculativeRetryValue none() {
            return new SpeculativeRetryValue("'NONE'");
        }

        /**
         * Always use speculative retry
         * @return speculative retry value
         */
        public static SpeculativeRetryValue always() {
            return new SpeculativeRetryValue("'ALWAYS'");
        }

        /**
         * Define a percentile for speculative retry. The percentile value should be between 0 and 100
         * @return speculative retry value
         */
        public static SpeculativeRetryValue percentile(int percentile) {
            if (percentile < 0 || percentile > 100) {
                throw new IllegalArgumentException("Percentile value for speculative retry should be between 0 and 100");
            }
            return new SpeculativeRetryValue("'" + percentile + "percentile'");
        }

        /**
         * Define a threshold in milli seconds for speculative retry
         * @return speculative retry value
         */
        public static SpeculativeRetryValue millisecs(int millisecs) {
            if (millisecs < 0) {
                throw new IllegalArgumentException("Millisecond value for speculative retry should be positive");
            }
            return new SpeculativeRetryValue("'" + millisecs + "ms'");
        }
    }

    /**
     * Define the number of rows to be cached per partition when Row Caching is enabled. This feature is only applicable to Cassandra 2.1.x
     * <ul>
     *     <li>NONE: Do not cache rows.</li>
     *     <li>ALL: cache all rows for a given partition.<strong>Be careful when choosing this option, you can starve quickly Cassandra memory if your partition is very large</strong></li>
     *     <li>int: define a number of rows to cache.</li>
     * </ul>
     *
     */
    public static class CachingRowsPerPartition {
        private String value;

        private CachingRowsPerPartition(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }

        /**
         * Do not cache rows
         * @return CachingRowsPerPartition
         */
        public static CachingRowsPerPartition none() {
            return new CachingRowsPerPartition("NONE");
        }

        /**
         * Cache all rows. <br/>
         * <strong>Be careful when choosing this option, you can starve quickly Cassandra memory if your partition is very large</strong>
         * @return CachingRowsPerPartition
         */
        public static CachingRowsPerPartition all() {
            return new CachingRowsPerPartition("ALL");
        }

        /**
         * Cache the given amount of rows
         * @param rowNumber number of rows to cache
         * @return CachingRowsPerPartition
         */
        public static CachingRowsPerPartition rows(int rowNumber) {
            if (rowNumber <= 0) {
                throw new IllegalArgumentException("rows number for caching should be strictly positive");
            }
            return new CachingRowsPerPartition(new Integer(rowNumber).toString());
        }
    }

    /**
     * For internal use only. Store a key/value of manual user-entered option
     */
    static class RawOption {
        final String key;
        final String value;

        RawOption(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }
}
