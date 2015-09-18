/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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
package com.datastax.driver.core;


import java.util.*;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Base class for Tables and Materialized Views.
 */
abstract class TableOrView {

    static final Comparator<ColumnMetadata> columnMetadataComparator = new Comparator<ColumnMetadata>() {
        public int compare(ColumnMetadata c1, ColumnMetadata c2) {
            return c1.getName().compareTo(c2.getName());
        }
    };

    protected final KeyspaceMetadata keyspace;
    protected final String name;
    protected final UUID id;
    protected final List<ColumnMetadata> partitionKey;
    protected final List<ColumnMetadata> clusteringColumns;
    protected final Map<String, ColumnMetadata> columns;
    protected final Options options;
    protected final List<Order> clusteringOrder;
    protected final VersionNumber cassandraVersion;

    /**
     * Clustering orders.
     * <p>
     * This is used by {@link #getClusteringOrder} to indicate the clustering
     * order of a table.
     */
    public enum Order {
        ASC, DESC;

        static final Predicate<Order> isAscending = new Predicate<Order>() {
            public boolean apply(Order o) {
                return o == ASC;
            }
        };
    }

    TableOrView(KeyspaceMetadata keyspace,
                        String name,
                        UUID id,
                        List<ColumnMetadata> partitionKey,
                        List<ColumnMetadata> clusteringColumns,
                        Map<String, ColumnMetadata> columns,
                        Options options,
                        List<Order> clusteringOrder,
                        VersionNumber cassandraVersion) {
        this.keyspace = keyspace;
        this.name = name;
        this.id = id;
        this.partitionKey = partitionKey;
        this.clusteringColumns = clusteringColumns;
        this.columns = columns;
        this.options = options;
        this.clusteringOrder = clusteringOrder;
        this.cassandraVersion = cassandraVersion;
    }

    protected static <T> List<T> nullInitializedList(int size) {
        List<T> l = new ArrayList<T>(size);
        for (int i = 0; i < size; ++i)
            l.add(null);
        return l;
    }

    /**
     * Returns the name of this table.
     *
     * @return the name of this CQL table.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the unique id of this table.
     * <p>
     * Note: this id is available in Cassandra 2.1 and above. It will be
     * {@code null} for earlier versions.
     *
     * @return the unique id of the table.
     */
    public UUID getId() {
        return id;
    }

    /**
     * Returns the keyspace this table belong to.
     *
     * @return the keyspace metadata of the keyspace this table belong to.
     */
    public KeyspaceMetadata getKeyspace() {
        return keyspace;
    }

    /**
     * Returns metadata on a column of this table.
     *
     * @param name the name of the column to retrieve ({@code name} will be
     * interpreted as a case-insensitive identifier unless enclosed in double-quotes,
     * see {@link Metadata#quote}).
     * @return the metadata for the column if it exists, or
     * {@code null} otherwise.
     */
    public ColumnMetadata getColumn(String name) {
        return columns.get(Metadata.handleId(name));
    }

    /**
     * Returns a list containing all the columns of this table.
     *
     * The order of the columns in the list is consistent with
     * the order of the columns returned by a {@code SELECT * FROM thisTable}:
     * the first column is the partition key, next are the clustering
     * columns in their defined order, and then the rest of the
     * columns follow in alphabetic order.
     *
     * @return a list containing the metadata for the columns of this table.
     */
    public List<ColumnMetadata> getColumns() {
        return new ArrayList<ColumnMetadata>(columns.values());
    }

    /**
     * Returns the list of columns composing the primary key for this table.
     *
     * A table will always at least have a partition key (that
     * may itself be one or more columns), so the returned list at least
     * has one element.
     *
     * @return the list of columns composing the primary key for this table.
     */
    public List<ColumnMetadata> getPrimaryKey() {
        List<ColumnMetadata> pk = new ArrayList<ColumnMetadata>(partitionKey.size() + clusteringColumns.size());
        pk.addAll(partitionKey);
        pk.addAll(clusteringColumns);
        return pk;
    }

    /**
     * Returns the list of columns composing the partition key for this table.
     *
     * A table always has a partition key so the returned list has
     * at least one element.
     *
     * @return the list of columns composing the partition key for this table.
     */
    public List<ColumnMetadata> getPartitionKey() {
        return Collections.unmodifiableList(partitionKey);
    }

    /**
     * Returns the list of clustering columns for this table.
     *
     * @return the list of clustering columns for this table.
     * If there is no clustering columns, an empty list is returned.
     */
    public List<ColumnMetadata> getClusteringColumns() {
        return Collections.unmodifiableList(clusteringColumns);
    }

    /**
     * Returns the clustering order for this table.
     * <p>
     * The returned contains the clustering order of each clustering column. The
     * {@code i}th element of the result correspond to the order (ascending or
     * descending) of the {@code i}th clustering column (see
     * {@link #getClusteringColumns}). Note that a table defined without any
     * particular clustering order is equivalent to one for which all the
     * clustering key are in ascending order.
     *
     * @return a list with the clustering order for each clustering column.
     */
    public List<Order> getClusteringOrder() {
        return clusteringOrder;
    }

    /**
     * Returns the options for this table.
     *
     * @return the options for this table.
     */
    public Options getOptions() {
        return options;
    }

    void add(ColumnMetadata column) {
        columns.put(column.getName(), column);
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * table and the index on it.
     * <p>
     * In other words, this method returns the queries that would allow you to
     * recreate the schema of this table, along with the indexes and views defined on
     * this table, if any.
     * <p>
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this table schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery(true));

        return sb.toString();
    }

    /**
     * Returns a CQL query representing this table.
     * <p>
     * This method returns a single 'CREATE TABLE' query with the options
     * corresponding to this table definition.
     * <p>
     * Note that the returned string is a single line; the returned query
     * is not formatted in any way.
     *
     * @return the 'CREATE TABLE' query corresponding to this table.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        return asCQLQuery(false);
    }

    protected abstract String asCQLQuery(boolean formatted);

    protected StringBuilder appendOptions(StringBuilder sb, boolean formatted){
        // Options
        sb.append(" WITH ");
        if (options.isCompactStorage)
            and(sb.append("COMPACT STORAGE"), formatted);
        if (!Iterables.all(clusteringOrder, Order.isAscending))
            and(appendClusteringOrder(sb), formatted);
        sb.append("read_repair_chance = ").append(options.readRepair);
        and(sb, formatted).append("dclocal_read_repair_chance = ").append(options.localReadRepair);
        if (cassandraVersion.getMajor() < 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() == 0))
            and(sb, formatted).append("replicate_on_write = ").append(options.replicateOnWrite);
        and(sb, formatted).append("gc_grace_seconds = ").append(options.gcGrace);
        and(sb, formatted).append("bloom_filter_fp_chance = ").append(options.bfFpChance);
        if (cassandraVersion.getMajor() < 2 || cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() < 1)
            and(sb, formatted).append("caching = '").append(options.caching.get("keys")).append('\'');
        else
            and(sb, formatted).append("caching = ").append(formatOptionMap(options.caching));
        if (options.comment != null)
            and(sb, formatted).append("comment = '").append(options.comment.replace("'","''")).append('\'');
        and(sb, formatted).append("compaction = ").append(formatOptionMap(options.compaction));
        and(sb, formatted).append("compression = ").append(formatOptionMap(options.compression));
        if (cassandraVersion.getMajor() >= 2) {
            and(sb, formatted).append("default_time_to_live = ").append(options.defaultTTL);
            and(sb, formatted).append("speculative_retry = '").append(options.speculativeRetry).append('\'');
            if (options.indexInterval != null)
                and(sb, formatted).append("index_interval = ").append(options.indexInterval);
        }
        if (cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1)) {
            and(sb, formatted).append("min_index_interval = ").append(options.minIndexInterval);
            and(sb, formatted).append("max_index_interval = ").append(options.maxIndexInterval);
        }
        if (cassandraVersion.getMajor() > 2) {
            and(sb, formatted).append("crc_check_chance = ").append(options.crcCheckChance);
        }
        sb.append(';');
        return sb;
    }

    @Override
    public String toString() {
        return asCQLQuery();
    }

    private StringBuilder appendClusteringOrder(StringBuilder sb) {
        sb.append("CLUSTERING ORDER BY (");
        for (int i = 0; i < clusteringColumns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(clusteringColumns.get(i).getName()).append(' ').append(clusteringOrder.get(i));
        }
        return sb.append(')');
    }

    private static String formatOptionMap(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        boolean first = true;
        for (Map.Entry<String, String> entry : m.entrySet()) {
            if (first) first = false; else sb.append(", ");
            sb.append('\'').append(entry.getKey()).append('\'');
            sb.append(" : ");
            try {
                sb.append(Integer.parseInt(entry.getValue()));
            } catch (NumberFormatException e) {
                sb.append('\'').append(entry.getValue()).append('\'');
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    private StringBuilder and(StringBuilder sb, boolean formatted) {
        return newLine(sb, formatted).append(spaces(2, formatted)).append(" AND ");
    }

    static String spaces(int n, boolean formatted) {
        if (!formatted)
            return "";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(' ');

        return sb.toString();
    }

    static StringBuilder newLine(StringBuilder sb, boolean formatted) {
        if (formatted)
            sb.append('\n');
        return sb;
    }

    static StringBuilder spaceOrNewLine(StringBuilder sb, boolean formatted) {
        sb.append(formatted ? '\n' : ' ');
        return sb;
    }

    public static class Options {

        private static final String COMMENT                     = "comment";
        private static final String READ_REPAIR                 = "read_repair_chance";
        private static final String DCLOCAL_READ_REPAIR         = "dclocal_read_repair_chance";
        private static final String LOCAL_READ_REPAIR           = "local_read_repair_chance";
        private static final String REPLICATE_ON_WRITE          = "replicate_on_write";
        private static final String GC_GRACE                    = "gc_grace_seconds";
        private static final String BF_FP_CHANCE                = "bloom_filter_fp_chance";
        private static final String CACHING                     = "caching";
        private static final String COMPACTION                  = "compaction";
        private static final String COMPACTION_CLASS            = "compaction_strategy_class";
        private static final String COMPACTION_OPTIONS          = "compaction_strategy_options";
        private static final String POPULATE_CACHE_ON_FLUSH     = "populate_io_cache_on_flush";
        private static final String COMPRESSION                 = "compression";
        private static final String COMPRESSION_PARAMS          = "compression_parameters";
        private static final String MEMTABLE_FLUSH_PERIOD_MS    = "memtable_flush_period_in_ms";
        private static final String DEFAULT_TTL                 = "default_time_to_live";
        private static final String SPECULATIVE_RETRY           = "speculative_retry";
        private static final String INDEX_INTERVAL              = "index_interval";
        private static final String MIN_INDEX_INTERVAL          = "min_index_interval";
        private static final String MAX_INDEX_INTERVAL          = "max_index_interval";
        private static final String CRC_CHECK_CHANCE            = "crc_check_chance";

        private static final boolean DEFAULT_REPLICATE_ON_WRITE = true;
        private static final double DEFAULT_BF_FP_CHANCE = 0.01;
        private static final boolean DEFAULT_POPULATE_CACHE_ON_FLUSH = false;
        private static final int DEFAULT_MEMTABLE_FLUSH_PERIOD = 0;
        private static final int DEFAULT_DEFAULT_TTL = 0;
        private static final String DEFAULT_SPECULATIVE_RETRY = "NONE";
        private static final int DEFAULT_INDEX_INTERVAL = 128;
        private static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
        private static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;
        private static final double DEFAULT_CRC_CHECK_CHANCE = 1.0;

        private final boolean isCompactStorage;

        private final String comment;
        private final double readRepair;
        private final double localReadRepair;
        private final boolean replicateOnWrite;
        private final int gcGrace;
        private final double bfFpChance;
        private final Map<String, String> caching;
        private final boolean populateCacheOnFlush;
        private final int memtableFlushPeriodMs;
        private final int defaultTTL;
        private final String speculativeRetry;
        private final Integer indexInterval;
        private final Integer minIndexInterval;
        private final Integer maxIndexInterval;
        private final Map<String, String> compaction;
        private final Map<String, String> compression;
        private final Double crcCheckChance;

        Options(Row row, boolean isCompactStorage, VersionNumber version) {

            boolean is120 = version.getMajor() < 2;
            boolean is200 = version.getMajor() == 2 && version.getMinor() == 0;
            boolean is210 = version.getMajor() == 2 && version.getMinor() >= 1;
            boolean is300OrHigher = version.getMajor() > 2;
            boolean is210OrHigher = is210 || is300OrHigher;

            this.isCompactStorage = isCompactStorage;
            this.comment = isNullOrAbsent(row, COMMENT) ? "" : row.getString(COMMENT);
            this.readRepair = row.getDouble(READ_REPAIR);

            if(is300OrHigher)
                this.localReadRepair = row.getDouble(DCLOCAL_READ_REPAIR);
            else
                this.localReadRepair = row.getDouble(LOCAL_READ_REPAIR);

            this.replicateOnWrite = is210OrHigher || isNullOrAbsent(row, REPLICATE_ON_WRITE) ? DEFAULT_REPLICATE_ON_WRITE : row.getBool(REPLICATE_ON_WRITE);
            this.gcGrace = row.getInt(GC_GRACE);
            this.bfFpChance = isNullOrAbsent(row, BF_FP_CHANCE) ? DEFAULT_BF_FP_CHANCE : row.getDouble(BF_FP_CHANCE);

            this.populateCacheOnFlush = isNullOrAbsent(row, POPULATE_CACHE_ON_FLUSH) ? DEFAULT_POPULATE_CACHE_ON_FLUSH : row.getBool(POPULATE_CACHE_ON_FLUSH);
            this.memtableFlushPeriodMs = is120 || isNullOrAbsent(row, MEMTABLE_FLUSH_PERIOD_MS) ? DEFAULT_MEMTABLE_FLUSH_PERIOD : row.getInt(MEMTABLE_FLUSH_PERIOD_MS);
            this.defaultTTL = is120 || isNullOrAbsent(row, DEFAULT_TTL) ? DEFAULT_DEFAULT_TTL : row.getInt(DEFAULT_TTL);
            this.speculativeRetry = is120 || isNullOrAbsent(row, SPECULATIVE_RETRY) ? DEFAULT_SPECULATIVE_RETRY : row.getString(SPECULATIVE_RETRY);

            if (is200)
                this.indexInterval = isNullOrAbsent(row, INDEX_INTERVAL) ? DEFAULT_INDEX_INTERVAL : row.getInt(INDEX_INTERVAL);
            else
                this.indexInterval = null;

            if (is210OrHigher) {
                this.minIndexInterval = isNullOrAbsent(row, MIN_INDEX_INTERVAL)
                                      ? DEFAULT_MIN_INDEX_INTERVAL
                                      : row.getInt(MIN_INDEX_INTERVAL);
                this.maxIndexInterval = isNullOrAbsent(row, MAX_INDEX_INTERVAL)
                                      ? DEFAULT_MAX_INDEX_INTERVAL
                                      : row.getInt(MAX_INDEX_INTERVAL);
            } else {
                this.minIndexInterval = null;
                this.maxIndexInterval = null;
            }

            if (is300OrHigher) {
                this.caching = ImmutableMap.copyOf(row.getMap(CACHING, String.class, String.class));
            } else if (is210) {
                this.caching = ImmutableMap.copyOf(SimpleJSONParser.parseStringMap(row.getString(CACHING)));
            } else {
                this.caching = ImmutableMap.of("keys", row.getString(CACHING));
            }

            if (is300OrHigher)
                this.compaction = ImmutableMap.copyOf(row.getMap(COMPACTION, String.class, String.class));
            else {
                this.compaction = ImmutableMap.<String, String>builder()
                    .put("class", row.getString(COMPACTION_CLASS))
                    .putAll(SimpleJSONParser.parseStringMap(row.getString(COMPACTION_OPTIONS)))
                    .build();
            }

            if(is300OrHigher)
                this.compression = ImmutableMap.copyOf(row.getMap(COMPRESSION, String.class, String.class));
            else
                this.compression = ImmutableMap.copyOf(SimpleJSONParser.parseStringMap(row.getString(COMPRESSION_PARAMS)));

            if(is300OrHigher)
                this.crcCheckChance = isNullOrAbsent(row, CRC_CHECK_CHANCE)
                    ? DEFAULT_CRC_CHECK_CHANCE
                    : row.getDouble(CRC_CHECK_CHANCE);
            else
                this.crcCheckChance = null;
        }

        private static boolean isNullOrAbsent(Row row, String name) {
            return row.getColumnDefinitions().getIndexOf(name) < 0
                   || row.isNull(name);
        }

        /**
         * Returns whether the table uses the {@code COMPACT STORAGE} option.
         *
         * @return whether the table uses the {@code COMPACT STORAGE} option.
         */
        public boolean isCompactStorage() {
            return isCompactStorage;
        }

        /**
         * Returns the commentary set for this table.
         *
         * @return the commentary set for this table, or {@code null} if noe has been set.
         */
        public String getComment() {
            return comment;
        }

        /**
         * Returns the chance with which a read repair is triggered for this table.
         *
         * @return the read repair chance set for table (in [0.0, 1.0]).
         */
        public double getReadRepairChance() {
            return readRepair;
        }

        /**
         * Returns the cluster local read repair chance set for this table.
         *
         * @return the local read repair chance set for table (in [0.0, 1.0]).
         */
        public double getLocalReadRepairChance() {
            return localReadRepair;
        }

        /**
         * Returns whether replicateOnWrite is set for this table.
         *
         * This is only meaningful for tables holding counters.
         *
         * @return whether replicateOnWrite is set for this table.
         */
        public boolean getReplicateOnWrite() {
            return replicateOnWrite;
        }

        /**
         * Returns the tombstone garbage collection grace time in seconds for this table.
         *
         * @return the tombstone garbage collection grace time in seconds for this table.
         */
        public int getGcGraceInSeconds() {
            return gcGrace;
        }

        /**
         * Returns the false positive chance for the Bloom filter of this table.
         *
         * @return the Bloom filter false positive chance for this table (in [0.0, 1.0]).
         */
        public double getBloomFilterFalsePositiveChance() {
            return bfFpChance;
        }

        /**
         * Returns the caching options for this table.
         *
         * @return an immutable map containing the caching options for this table.
         */
        public Map<String, String> getCaching() {
            return caching;
        }

        /**
         * Whether the populate I/O cache on flush is set on this table.
         *
         * @return whether the populate I/O cache on flush is set on this table.
         */
        public boolean getPopulateIOCacheOnFlush() {
            return populateCacheOnFlush;
        }

        /*
         * Returns the memtable flush period (in milliseconds) option for this table.
         * <p>
         * Note: this option is not available in Cassandra 1.2 and will return 0 (no periodic
         * flush) when connected to 1.2 nodes.
         *
         * @return the memtable flush period option for this table or 0 if no
         * periodic flush is configured.
         */
        public int getMemtableFlushPeriodInMs() {
            return memtableFlushPeriodMs;
        }

        /**
         * Returns the default TTL for this table.
         * <p>
         * Note: this option is not available in Cassandra 1.2 and will return 0 (no default
         * TTL) when connected to 1.2 nodes.
         *
         * @return the default TTL for this table or 0 if no default TTL is
         * configured.
         */
        public int getDefaultTimeToLive() {
            return defaultTTL;
        }

        /**
         * Returns the speculative retry option for this table.
         * <p>
         * Note: this option is not available in Cassandra 1.2 and will return "NONE" (no
         * speculative retry) when connected to 1.2 nodes.
         *
         * @return the speculative retry option this table.
         */
        public String getSpeculativeRetry() {
            return speculativeRetry;
        }

        /**
         * Returns the index interval option for this table.
         * <p>
         * Note: this option is not available in Cassandra 1.2 (more precisely, it is not
         * configurable per-table) and will return 128 (the default index interval) when
         * connected to 1.2 nodes. It is deprecated in Cassandra 2.1 and above, and will
         * therefore return {@code null} for 2.1 nodes.
         *
         * @return the index interval option for this table.
         */
        public Integer getIndexInterval() {
            return indexInterval;
        }

        /**
         * Returns the minimum index interval option for this table.
         * <p>
         * Note: this option is available in Cassandra 2.1 and above, and will return
         * {@code null} for earlier versions.
         *
         * @return the minimum index interval option for this table.
         */
        public Integer getMinIndexInterval() {
            return minIndexInterval;
        }

        /**
         * Returns the maximum index interval option for this table.
         * <p>
         * Note: this option is available in Cassandra 2.1 and above, and will return
         * {@code null} for earlier versions.
         *
         * @return the maximum index interval option for this table.
         */
        public Integer getMaxIndexInterval() {
            return maxIndexInterval;
        }

        /**
         * When compression is enabled, this option defines the probability
         * with which checksums for compressed blocks are checked during reads.
         * The default value for this options is 1.0 (always check).
         * <p>
         * Note that this option is available in Cassandra 3.0.0 and above, when it
         * became a "top-level" table option, whereas previously it was a suboption
         * of the {@link #getCompression() compression} option.
         * <p>
         * For Cassandra versions prior to 3.0.0, this method always returns {@code null}.
         *
         * @return the probability with which checksums for compressed blocks are checked during reads
         */
        public Double getCrcCheckChance() {
            return crcCheckChance;
        }

        /**
         * Returns the compaction options for this table.
         *
         * @return an immutable map containing the compaction options for this table.
         */
        public Map<String, String> getCompaction() {
            return compaction;
        }

        /**
         * Returns the compression options for this table.
         *
         * @return an immutable map containing the compression options for this table.
         */
        public Map<String, String> getCompression() {
            return compression;
        }

        @Override
        public boolean equals(Object other) {
            if (other == this)
                return true;
            if (!(other instanceof Options))
                return false;

            Options that = (Options)other;
            return this.isCompactStorage == that.isCompactStorage &&
                Objects.equal(this.comment, that.comment) &&
                this.readRepair == that.readRepair &&
                this.localReadRepair == that.localReadRepair &&
                this.replicateOnWrite == that.replicateOnWrite &&
                this.gcGrace == that.gcGrace &&
                this.bfFpChance == that.bfFpChance &&
                Objects.equal(this.caching, that.caching) &&
                this.populateCacheOnFlush == that.populateCacheOnFlush &&
                this.memtableFlushPeriodMs == that.memtableFlushPeriodMs &&
                this.defaultTTL == that.defaultTTL &&
                Objects.equal(this.speculativeRetry, that.speculativeRetry) &&
                Objects.equal(this.indexInterval, that.indexInterval) &&
                Objects.equal(this.minIndexInterval, that.minIndexInterval) &&
                Objects.equal(this.maxIndexInterval, that.maxIndexInterval) &&
                Objects.equal(this.compaction, that.compaction) &&
                Objects.equal(this.compression, that.compression);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(isCompactStorage, comment, readRepair, localReadRepair, replicateOnWrite, gcGrace,
                bfFpChance, caching, populateCacheOnFlush, memtableFlushPeriodMs, defaultTTL, speculativeRetry,
                indexInterval, minIndexInterval, maxIndexInterval, compaction, compression);
        }
    }
}
