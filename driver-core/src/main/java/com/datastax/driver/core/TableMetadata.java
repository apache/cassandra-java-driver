/*
 *      Copyright (C) 2012 DataStax Inc.
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
import java.util.regex.Pattern;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Describes a Table.
 */
public class TableMetadata {

    static final String CF_NAME                      = "columnfamily_name";

    private static final String KEY_VALIDATOR        = "key_validator";
    private static final String COMPARATOR           = "comparator";
    private static final String VALIDATOR            = "default_validator";

    private static final String DEFAULT_KEY_ALIAS    = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS  = "value";

    private static final Pattern lowercaseId = Pattern.compile("[a-z][a-z0-9_]*");

    private static final Comparator<ColumnMetadata> columnMetadataComparator = new Comparator<ColumnMetadata>() {
        public int compare(ColumnMetadata c1, ColumnMetadata c2) {
            return c1.getName().compareTo(c2.getName());
        }
    };

    private final KeyspaceMetadata keyspace;
    private final String name;
    private final List<ColumnMetadata> partitionKey;
    private final List<ColumnMetadata> clusteringColumns;
    private final Map<String, ColumnMetadata> columns;
    private final Options options;
    private final List<Order> clusteringOrder;

    /**
     * Clustering orders.
     * <p>
     * This is used by {@link #getClusteringOrder} to indicate the clustering
     * order of a table.
     */
    public static enum Order {
        ASC, DESC;

        static final Predicate<Order> isAscending = new Predicate<Order>() {
            public boolean apply(Order o) {
                return o == ASC;
            }
        };
    }

    private TableMetadata(KeyspaceMetadata keyspace,
                          String name,
                          List<ColumnMetadata> partitionKey,
                          List<ColumnMetadata> clusteringColumns,
                          LinkedHashMap<String, ColumnMetadata> columns,
                          Options options,
                          List<Order> clusteringOrder) {
        this.keyspace = keyspace;
        this.name = name;
        this.partitionKey = partitionKey;
        this.clusteringColumns = clusteringColumns;
        this.columns = columns;
        this.options = options;
        this.clusteringOrder = clusteringOrder;
    }

    static TableMetadata build(KeyspaceMetadata ksm, Row row, Map<String, ColumnMetadata.Raw> rawCols) {

        String name = row.getString(CF_NAME);

        CassandraTypeParser.ParseResult comparator = CassandraTypeParser.parseWithComposite(row.getString(COMPARATOR));
        CassandraTypeParser.ParseResult keyValidator = CassandraTypeParser.parseWithComposite(row.getString(KEY_VALIDATOR));

        int clusteringSize = findClusteringSize(rawCols.values());
        boolean isDense = clusteringSize != comparator.types.size() - 1;
        boolean isCompact = isDense || !comparator.isComposite;

        List<ColumnMetadata> partitionKey = nullInitializedList(keyValidator.types.size());
        List<ColumnMetadata> clusteringColumns = nullInitializedList(clusteringSize);
        List<Order> clusteringOrder = nullInitializedList(clusteringSize);
        // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();

        TableMetadata tm = new TableMetadata(ksm, name, partitionKey, clusteringColumns, columns, new Options(row, isCompact), clusteringOrder);

        // We use this temporary set just so non PK columns are added in lexicographical order, which is the one of a
        // 'SELECT * FROM ...'
        Set<ColumnMetadata> otherColumns = new TreeSet<ColumnMetadata>(columnMetadataComparator);
        for (ColumnMetadata.Raw rawCol : rawCols.values()) {
            ColumnMetadata col = ColumnMetadata.fromRaw(tm, rawCol);
            otherColumns.add(col);
            switch (rawCol.kind) {
                case PARTITION_KEY:
                    partitionKey.set(rawCol.componentIndex, col);
                    break;
                case CLUSTERING_KEY:
                    clusteringColumns.set(rawCol.componentIndex, col);
                    clusteringOrder.set(rawCol.componentIndex, rawCol.isReversed ? Order.DESC : Order.ASC);
                    break;
            }
        }

        for (ColumnMetadata c : partitionKey)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : clusteringColumns)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : otherColumns)
            columns.put(c.getName(), c);

        ksm.add(tm);
        return tm;
    }

    private static int findClusteringSize(Collection<ColumnMetadata.Raw> cols) {
        int maxId = -1;
        for (ColumnMetadata.Raw col : cols)
            if (col.kind == ColumnMetadata.Raw.Kind.CLUSTERING_KEY)
                maxId = Math.max(maxId, col.componentIndex);
        return maxId + 1;
    }

    private static <T> List<T> nullInitializedList(int size) {
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
     * @param name the name of the column to retrieve.
     * @return the metadata for the {@code name} column if it exists, or
     * {@code null} otherwise.
     */
    public ColumnMetadata getColumn(String name) {
        return columns.get(name);
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

    // :_(
    private static ObjectMapper jsonMapper = new ObjectMapper(new JsonFactory());

    @SuppressWarnings("unchecked")
    static Map<String, String> fromJsonMap(String json) {
        try {
            return jsonMapper.readValue(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void add(ColumnMetadata column) {
        columns.put(column.getName(), column);
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * table and the index on it.
     *
     * In other words, this method returns the queries that would allow you to
     * recreate the schema of this table, along with the index defined on
     * columns of this table.
     *
     * Note that the returned String is formatted to be human readable (for
     * some definition of human readable at least).
     *
     * @return the CQL queries representing this table schema as a {code
     * String}.
     */
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(asCQLQuery(true));

        for (ColumnMetadata column : columns.values()) {
            ColumnMetadata.IndexMetadata index = column.getIndex();
            if (index == null)
                continue;

            sb.append("\n").append(index.asCQLQuery());
        }
        return sb.toString();
    }

    /**
     * Returns a CQL query representing this table.
     *
     * This method returns a single 'CREATE TABLE' query with the options
     * corresponding to this table definition.
     *
     * Note that the returned string is a single line; the returned query
     * is not formatted in any way.
     *
     * @return the 'CREATE TABLE' query corresponding to this table.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        return asCQLQuery(false);
    }

    // Escape a CQL3 identifier based on its value as read from the schema
    // tables. Because it cames from Cassandra, we could just always quote it,
    // but to get a nicer output we don't do it if it's not necessary.
    static String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseId.matcher(ident).matches() ? ident : '"' + ident + '"';
    }

    private String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(escapeId(keyspace.getName())).append(".").append(escapeId(name)).append(" (");
        newLine(sb, formatted);
        for (ColumnMetadata cm : columns.values())
            newLine(sb.append(spaces(4, formatted)).append(cm).append(","), formatted);

        // PK
        sb.append(spaces(4, formatted)).append("PRIMARY KEY (");
        if (partitionKey.size() == 1) {
            sb.append(partitionKey.get(0).getName());
        } else {
            sb.append("(");
            boolean first = true;
            for (ColumnMetadata cm : partitionKey) {
                if (first) first = false; else sb.append(", ");
                sb.append(escapeId(cm.getName()));
            }
            sb.append(")");
        }
        for (ColumnMetadata cm : clusteringColumns)
            sb.append(", ").append(escapeId(cm.getName()));
        sb.append(")");
        newLine(sb, formatted);
        // end PK

        // Options
        sb.append(") WITH ");

        if (options.isCompactStorage)
            and(sb.append("COMPACT STORAGE"), formatted);
        if (!Iterables.all(clusteringOrder, Order.isAscending))
            and(appendClusteringOrder(sb), formatted);
        sb.append("read_repair_chance = ").append(options.readRepair);
        and(sb, formatted).append("dclocal_read_repair_chance = ").append(options.localReadRepair);
        and(sb, formatted).append("replicate_on_write = ").append(options.replicateOnWrite);
        and(sb, formatted).append("gc_grace_seconds = ").append(options.gcGrace);
        and(sb, formatted).append("bloom_filter_fp_chance = ").append(options.bfFpChance);
        and(sb, formatted).append("caching = '").append(options.caching).append("'");
        if (options.comment != null)
            and(sb, formatted).append("comment = '").append(options.comment).append("'");
        and(sb, formatted).append("compaction = ").append(formatOptionMap(options.compaction));
        and(sb, formatted).append("compression = ").append(formatOptionMap(options.compression));
        sb.append(";");
        return sb.toString();
    }

    private StringBuilder appendClusteringOrder(StringBuilder sb) {
        sb.append("CLUSTERING ORDER BY (");
        for (int i = 0; i < clusteringColumns.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(clusteringColumns.get(i).getName()).append(" ").append(clusteringOrder.get(i));
        }
        return sb.append(")");
    }

    private static String formatOptionMap(Map<String, String> m) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        boolean first = true;
        for (Map.Entry<String, String> entry : m.entrySet()) {
            if (first) first = false; else sb.append(", ");
            sb.append("'").append(entry.getKey()).append("'");
            sb.append(" : ");
            try {
                sb.append(Integer.parseInt(entry.getValue()));
            } catch (NumberFormatException e) {
                sb.append("'").append(entry.getValue()).append("'");
            }
        }
        sb.append(" }");
        return sb.toString();
    }

    private StringBuilder and(StringBuilder sb, boolean formatted) {
        return newLine(sb, formatted).append(spaces(2, formatted)).append(" AND ");
    }

    private String spaces(int n, boolean formatted) {
        if (!formatted)
            return "";

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++)
            sb.append(' ');

        return sb.toString();
    }

    private StringBuilder newLine(StringBuilder sb, boolean formatted) {
        if (formatted)
            sb.append('\n');
        return sb;
    }


    public static class Options {

        private static final String COMMENT                  = "comment";
        private static final String READ_REPAIR              = "read_repair_chance";
        private static final String LOCAL_READ_REPAIR        = "local_read_repair_chance";
        private static final String REPLICATE_ON_WRITE       = "replicate_on_write";
        private static final String GC_GRACE                 = "gc_grace_seconds";
        private static final String BF_FP_CHANCE             = "bloom_filter_fp_chance";
        private static final String CACHING                  = "caching";
        private static final String COMPACTION_CLASS         = "compaction_strategy_class";
        private static final String COMPACTION_OPTIONS       = "compaction_strategy_options";
        private static final String MIN_COMPACTION_THRESHOLD = "min_compaction_threshold";
        private static final String MAX_COMPACTION_THRESHOLD = "max_compaction_threshold";
        private static final String POPULATE_CACHE_ON_FLUSH  = "populate_io_cache_on_flush";
        private static final String COMPRESSION_PARAMS       = "compression_parameters";
        private static final String MEMTABLE_FLUSH_PERIOD_MS = "memtable_flush_period_in_ms";
        private static final String DEFAULT_TTL              = "default_time_to_live";
        private static final String SPECULATIVE_RETRY        = "speculative_retry";
        private static final String INDEX_INTERVAL           = "index_interval";

        private static final double DEFAULT_BF_FP_CHANCE = 0.01;
        private static final boolean DEFAULT_POPULATE_CACHE_ON_FLUSH = false;
        private static final int DEFAULT_MEMTABLE_FLUSH_PERIOD = 0;
        private static final int DEFAULT_DEFAULT_TTL = 0;
        private static final String DEFAULT_SPECULATIVE_RETRY = "NONE";
        private static final int DEFAULT_INDEX_INTERVAL = 128;

        private final boolean isCompactStorage;

        private final String comment;
        private final double readRepair;
        private final double localReadRepair;
        private final boolean replicateOnWrite;
        private final int gcGrace;
        private final double bfFpChance;
        private final String caching;
        private final boolean populateCacheOnFlush;
        private final int memtableFlushPeriodMs;
        private final int defaultTTL;
        private final String speculativeRetry;
        private final int indexInterval;
        private final Map<String, String> compaction = new HashMap<String, String>();
        private final Map<String, String> compression = new HashMap<String, String>();

        Options(Row row, boolean isCompactStorage) {
            this.isCompactStorage = isCompactStorage;
            this.comment = row.isNull(COMMENT) ? "" : row.getString(COMMENT);
            this.readRepair = row.getDouble(READ_REPAIR);
            this.localReadRepair = row.getDouble(LOCAL_READ_REPAIR);
            this.replicateOnWrite = row.getBool(REPLICATE_ON_WRITE);
            this.gcGrace = row.getInt(GC_GRACE);
            this.bfFpChance = row.isNull(BF_FP_CHANCE) ? DEFAULT_BF_FP_CHANCE : row.getDouble(BF_FP_CHANCE);
            this.caching = row.getString(CACHING);
            this.populateCacheOnFlush = row.isNull(POPULATE_CACHE_ON_FLUSH) ? DEFAULT_POPULATE_CACHE_ON_FLUSH : row.getBool(POPULATE_CACHE_ON_FLUSH);
            this.memtableFlushPeriodMs = row.isNull(MEMTABLE_FLUSH_PERIOD_MS) ? DEFAULT_MEMTABLE_FLUSH_PERIOD : row.getInt(MEMTABLE_FLUSH_PERIOD_MS);
            this.defaultTTL = row.isNull(DEFAULT_TTL) ? DEFAULT_DEFAULT_TTL : row.getInt(DEFAULT_TTL);
            this.speculativeRetry = row.isNull(SPECULATIVE_RETRY) ? DEFAULT_SPECULATIVE_RETRY : row.getString(SPECULATIVE_RETRY);
            this.indexInterval = row.isNull(INDEX_INTERVAL) ? DEFAULT_INDEX_INTERVAL : row.getInt(INDEX_INTERVAL);

            this.compaction.put("class", row.getString(COMPACTION_CLASS));
            this.compaction.putAll(fromJsonMap(row.getString(COMPACTION_OPTIONS)));

            this.compression.putAll(fromJsonMap(row.getString(COMPRESSION_PARAMS)));
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
         * @return the read repair change set for table (in [0.0, 1.0]).
         */
        public double getReadRepairChance() {
            return readRepair;
        }

        /**
         * Returns the cluster local read repair chance set for this table.
         *
         * @return the local read repair change set for table (in [0.0, 1.0]).
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
         * Returns the caching option for this table.
         *
         * @return the caching option for this table.
         */
        public String getCaching() {
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
         *
         * @return the memtable flush period option for this table or 0 if no
         * periodic flush is configured.
         */
        public int getMemtableFlushPeriodInMs() {
            return memtableFlushPeriodMs;
        }

        /**
         * Returns the default TTL for this table.
         *
         * @return the default TTL for this table or 0 if no default TTL is
         * configured.
         */
        public int getDefaultTimeToLive() {
            return defaultTTL;
        }

        /**
         * Returns the speculative retry option for this table.
         *
         * @return the speculative retry option this table.
         */
        public String getSpeculativeRetry() {
            return speculativeRetry;
        }

        /**
         * Returns the index interval option for this table.
         *
         * @return the index interval option for this table.
         */
        public int getIndexInterval() {
            return indexInterval;
        }

        /**
         * Returns the compaction options for this table.
         *
         * @return a map containing the compaction options for this table.
         */
        public Map<String, String> getCompaction() {
            return new HashMap<String, String>(compaction);
        }

        /**
         * Returns the compression options for this table.
         *
         * @return a map containing the compression options for this table.
         */
        public Map<String, String> getCompression() {
            return new HashMap<String, String>(compression);
        }
    }
}
