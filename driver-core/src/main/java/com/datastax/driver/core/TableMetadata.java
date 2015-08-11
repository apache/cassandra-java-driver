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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes a Table.
 */
public class TableMetadata {

    private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);

    private static final String CF_ID_V2             = "cf_id";
    private static final String CF_ID_V3             = "id";

    private static final String KEY_VALIDATOR        = "key_validator";
    private static final String COMPARATOR           = "comparator";
    private static final String VALIDATOR            = "default_validator";

    private static final String KEY_ALIASES          = "key_aliases";
    private static final String COLUMN_ALIASES       = "column_aliases";
    private static final String VALUE_ALIAS          = "value_alias";

    private static final String DEFAULT_KEY_ALIAS    = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS  = "value";

    private static final String FLAGS                = "flags";
    private static final String DENSE                = "dense";
    private static final String SUPER                = "super";
    private static final String COMPOUND             = "compound";

    private static final String EMPTY_TYPE           = "org.apache.cassandra.db.marshal.EmptyType";

    private static final Comparator<ColumnMetadata> columnMetadataComparator = new Comparator<ColumnMetadata>() {
        public int compare(ColumnMetadata c1, ColumnMetadata c2) {
            return c1.getName().compareTo(c2.getName());
        }
    };

    private final KeyspaceMetadata keyspace;
    private final String name;
    private final UUID id;
    private final List<ColumnMetadata> partitionKey;
    private final List<ColumnMetadata> clusteringColumns;
    private final Map<String, ColumnMetadata> columns;
    private final Options options;
    private final List<Order> clusteringOrder;

    private final VersionNumber cassandraVersion;

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
                          UUID id,
                          List<ColumnMetadata> partitionKey,
                          List<ColumnMetadata> clusteringColumns,
                          LinkedHashMap<String, ColumnMetadata> columns,
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

    static TableMetadata build(KeyspaceMetadata ksm, Row row, Map<String, ColumnMetadata.Raw> rawCols, String nameColumn, VersionNumber cassandraVersion, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {

        String name = row.getString(nameColumn);

        UUID id = null;

        if(cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1)
            id = row.getUUID(CF_ID_V2);
        else if(cassandraVersion.getMajor() > 2)
            id = row.getUUID(CF_ID_V3);

        CassandraTypeParser.ParseResult comparator = null;
        CassandraTypeParser.ParseResult keyValidator = null;
        List<String> columnAliases = null;

        if(cassandraVersion.getMajor() <= 2) {
            comparator = CassandraTypeParser.parseWithComposite(row.getString(COMPARATOR), protocolVersion, codecRegistry);
            keyValidator = CassandraTypeParser.parseWithComposite(row.getString(KEY_VALIDATOR), protocolVersion, codecRegistry);
            columnAliases = cassandraVersion.getMajor() >= 2 || row.getString(COLUMN_ALIASES) == null
                ? Collections.<String>emptyList()
                : SimpleJSONParser.parseStringList(row.getString(COLUMN_ALIASES));
        }

        int partitionKeySize = findPartitionKeySize(rawCols.values(), keyValidator);
        int clusteringSize;

        boolean isDense;
        boolean isCompact;
        if(cassandraVersion.getMajor() > 2) {
            Set<String> flags = row.getSet(FLAGS, String.class);
            isDense = flags.contains(DENSE);
            boolean isSuper = flags.contains(SUPER);
            boolean isCompound = flags.contains(COMPOUND);
            isCompact = isSuper || isDense || !isCompound;
            boolean isStaticCompact = !isSuper && !isDense && !isCompound;
            if(isStaticCompact) {
                rawCols = pruneStaticCompactTableColumns(rawCols);
            }
            if(isDense) {
                rawCols = pruneDenseTableColumnsV3(rawCols);
            }
            clusteringSize = findClusteringSize(comparator, rawCols.values(), columnAliases, cassandraVersion);
        } else {
            assert comparator != null;
            clusteringSize = findClusteringSize(comparator, rawCols.values(), columnAliases, cassandraVersion);
            isDense = clusteringSize != comparator.types.size() - 1;
            if(isDense) {
                rawCols = pruneDenseTableColumnsV2(rawCols);
            }
            isCompact = isDense || !comparator.isComposite;
        }

        List<ColumnMetadata> partitionKey = nullInitializedList(partitionKeySize);
        List<ColumnMetadata> clusteringColumns = nullInitializedList(clusteringSize);
        List<Order> clusteringOrder = nullInitializedList(clusteringSize);

        // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();

        Options options = null;
        try {
            options = new Options(row, isCompact, cassandraVersion);
        } catch (RuntimeException e) {
            // See ControlConnection#refreshSchema for why we'd rather not probably this further. Since table options is one thing
            // that tends to change often in Cassandra, it's worth special casing this.
            logger.error(String.format("Error parsing schema options for table %s.%s: "
                                       + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\").getOptions() will return null",
                                       ksm.getName(), name, ksm.getName(), name), e);
        }

        TableMetadata tm = new TableMetadata(ksm, name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, cassandraVersion);

        // We use this temporary set just so non PK columns are added in lexicographical order, which is the one of a
        // 'SELECT * FROM ...'
        Set<ColumnMetadata> otherColumns = new TreeSet<ColumnMetadata>(columnMetadataComparator);

        if (cassandraVersion.getMajor() < 2) {

            assert comparator != null;
            assert keyValidator != null;
            assert columnAliases != null;

            // In C* 1.2, only the REGULAR columns are in the columns schema table, so we need to add the names from
            // the aliases (and make sure we handle default aliases).
            List<String> keyAliases = row.getString(KEY_ALIASES) == null
                                    ? Collections.<String>emptyList()
                                    : SimpleJSONParser.parseStringList(row.getString(KEY_ALIASES));
            for (int i = 0; i < partitionKey.size(); i++) {
                String alias = keyAliases.size() > i ? keyAliases.get(i) : (i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1));
                partitionKey.set(i, ColumnMetadata.forAlias(tm, alias, keyValidator.types.get(i)));
            }

            for (int i = 0; i < clusteringSize; i++) {
                String alias = columnAliases.size() > i ? columnAliases.get(i) : DEFAULT_COLUMN_ALIAS + (i + 1);
                clusteringColumns.set(i, ColumnMetadata.forAlias(tm, alias, comparator.types.get(i)));
                clusteringOrder.set(i, comparator.reversed.get(i) ? Order.DESC : Order.ASC);
            }

            // if we're dense, chances are that we have a single regular "value" column with an alias
            if (isDense) {
                String alias = row.isNull(VALUE_ALIAS) ? DEFAULT_VALUE_ALIAS : row.getString(VALUE_ALIAS);
                // ...unless the table does not have any regular column, only primary key columns (JAVA-873)
                if(!alias.isEmpty()) {
                    DataType type = CassandraTypeParser.parseOne(row.getString(VALIDATOR), protocolVersion, codecRegistry);
                    otherColumns.add(ColumnMetadata.forAlias(tm, alias, type));
                }
            }
        }

        for (ColumnMetadata.Raw rawCol : rawCols.values()) {
            ColumnMetadata col = ColumnMetadata.fromRaw(tm, rawCol);
            switch (rawCol.kind) {
                case PARTITION_KEY:
                    partitionKey.set(rawCol.componentIndex, col);
                    break;
                case CLUSTERING_COLUMN:
                    clusteringColumns.set(rawCol.componentIndex, col);
                    clusteringOrder.set(rawCol.componentIndex, rawCol.isReversed ? Order.DESC : Order.ASC);
                    break;
                default:
                    otherColumns.add(col);
                    break;
            }
        }

        for (ColumnMetadata c : partitionKey)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : clusteringColumns)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : otherColumns)
            columns.put(c.getName(), c);

        return tm;
    }

    /**
     * Upon migration from thrift to CQL, we internally create a pair of surrogate clustering/regular columns
     * for compact static tables. These columns shouldn't be exposed to the user but are currently returned by C*.
     * We also need to remove the static keyword for all other columns in the table.
     */
    private static Map<String, ColumnMetadata.Raw> pruneStaticCompactTableColumns(Map<String, ColumnMetadata.Raw> rawCols) {
        Collection<ColumnMetadata.Raw> cols = rawCols.values();
        Iterator<ColumnMetadata.Raw> it = cols.iterator();
        while(it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if(col.kind == ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN) {
                // remove "column1 text" clustering column
                it.remove();
            } else if(col.kind == ColumnMetadata.Raw.Kind.REGULAR) {
                // remove "value blob" regular column
                it.remove();
            } else if(col.kind == ColumnMetadata.Raw.Kind.STATIC) {
                // remove "static" keyword
                col.kind = ColumnMetadata.Raw.Kind.REGULAR;
            }
        }
        return rawCols;
    }

    /**
     * Upon migration from thrift to CQL, we internally create a surrogate column "value" of type
     * EmptyType for dense tables. This column shouldn't be exposed to the user but is currently returned by C*.
     */
    private static Map<String, ColumnMetadata.Raw> pruneDenseTableColumnsV3(Map<String, ColumnMetadata.Raw> rawCols) {
        Collection<ColumnMetadata.Raw> cols = rawCols.values();
        Iterator<ColumnMetadata.Raw> it = cols.iterator();
        while(it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if(col.kind == ColumnMetadata.Raw.Kind.REGULAR && col.dataType instanceof DataType.CustomType && EMPTY_TYPE.equals(((DataType.CustomType)col.dataType).getCustomTypeClassName())) {
                // remove "value EmptyType" regular column
                it.remove();
            }
        }
        return rawCols;
    }

    private static Map<String, ColumnMetadata.Raw> pruneDenseTableColumnsV2(Map<String, ColumnMetadata.Raw> rawCols) {
        Collection<ColumnMetadata.Raw> cols = rawCols.values();
        Iterator<ColumnMetadata.Raw> it = cols.iterator();
        while(it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if(col.kind == ColumnMetadata.Raw.Kind.COMPACT_VALUE && col.name.isEmpty()) {
                // remove "" blob regular COMPACT_VALUE column
                it.remove();
            }
        }
        return rawCols;
    }

    private static int findPartitionKeySize(Collection<ColumnMetadata.Raw> cols, CassandraTypeParser.ParseResult keyValidator) {
        // C* 1.2, 2.0, 2.1 and 2.2
        if(keyValidator != null)
            return keyValidator.types.size();
        // C* 3.0 onwards
        int maxId = -1;
        for (ColumnMetadata.Raw col : cols)
            if (col.kind == ColumnMetadata.Raw.Kind.PARTITION_KEY)
                maxId = Math.max(maxId, col.componentIndex);
        return maxId + 1;
    }

    private static int findClusteringSize(CassandraTypeParser.ParseResult comparator,
                                          Collection<ColumnMetadata.Raw> cols,
                                          List<String> columnAliases,
                                          VersionNumber cassandraVersion) {
        // In 2.0 onwards, this is relatively easy, we just find the biggest 'componentIndex' amongst the clustering columns.
        // For 1.2 however, this is slightly more subtle: we need to infer it based on whether the comparator is composite or not, and whether we have
        // regular columns or not.
        if (cassandraVersion.getMajor() >= 2) {
            int maxId = -1;
            for (ColumnMetadata.Raw col : cols)
                if (col.kind == ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN)
                    maxId = Math.max(maxId, col.componentIndex);
            return maxId + 1;
        } else {
            int size = comparator.types.size();
            if (comparator.isComposite)
                return !comparator.collections.isEmpty() || (columnAliases.size() == size - 1 && comparator.types.get(size - 1).equals(DataType.text())) ? size - 1 : size;
            else
                // We know cols only has the REGULAR ones for 1.2
                return !columnAliases.isEmpty() || cols.isEmpty() ? size : 0;
        }
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
     * @return the metadata for the {@code name} column if it exists, or
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
     * recreate the schema of this table, along with the index defined on
     * columns of this table.
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

        for (ColumnMetadata column : columns.values()) {
            ColumnMetadata.IndexMetadata index = column.getIndex();
            if (index == null)
                continue;

            sb.append('\n').append(index.asCQLQuery());
        }
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

    private String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(Metadata.escapeId(keyspace.getName())).append('.').append(Metadata.escapeId(name)).append(" (");
        newLine(sb, formatted);
        for (ColumnMetadata cm : columns.values())
            newLine(sb.append(spaces(4, formatted)).append(cm).append(','), formatted);

        // PK
        sb.append(spaces(4, formatted)).append("PRIMARY KEY (");
        if (partitionKey.size() == 1) {
            sb.append(partitionKey.get(0).getName());
        } else {
            sb.append('(');
            boolean first = true;
            for (ColumnMetadata cm : partitionKey) {
                if (first) first = false; else sb.append(", ");
                sb.append(Metadata.escapeId(cm.getName()));
            }
            sb.append(')');
        }
        for (ColumnMetadata cm : clusteringColumns)
            sb.append(", ").append(Metadata.escapeId(cm.getName()));
        sb.append(')');
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
        sb.append(';');
        return sb.toString();
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

        private static final boolean DEFAULT_REPLICATE_ON_WRITE = true;
        private static final double DEFAULT_BF_FP_CHANCE = 0.01;
        private static final boolean DEFAULT_POPULATE_CACHE_ON_FLUSH = false;
        private static final int DEFAULT_MEMTABLE_FLUSH_PERIOD = 0;
        private static final int DEFAULT_DEFAULT_TTL = 0;
        private static final String DEFAULT_SPECULATIVE_RETRY = "NONE";
        private static final int DEFAULT_INDEX_INTERVAL = 128;
        private static final int DEFAULT_MIN_INDEX_INTERVAL = 128;
        private static final int DEFAULT_MAX_INDEX_INTERVAL = 2048;

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
    }
}
