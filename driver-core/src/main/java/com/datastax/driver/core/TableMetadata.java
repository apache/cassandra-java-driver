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

import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.db.marshal.*;

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

    private static final String KEY_ALIASES          = "key_aliases";
    private static final String COLUMN_ALIASES       = "column_aliases";
    private static final String VALUE_ALIAS          = "value_alias";

    private static final String DEFAULT_KEY_ALIAS    = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS  = "value";

    private final KeyspaceMetadata keyspace;
    private final String name;
    private final List<ColumnMetadata> partitionKey;
    private final List<ColumnMetadata> clusteringKey;
    private final Map<String, ColumnMetadata> columns;
    private final Options options;

    private TableMetadata(KeyspaceMetadata keyspace,
                          String name,
                          List<ColumnMetadata> partitionKey,
                          List<ColumnMetadata> clusteringKey,
                          LinkedHashMap<String, ColumnMetadata> columns,
                          Options options) {
        this.keyspace = keyspace;
        this.name = name;
        this.partitionKey = partitionKey;
        this.clusteringKey = clusteringKey;
        this.columns = columns;
        this.options = options;
    }

    static TableMetadata build(KeyspaceMetadata ksm, Row row, boolean hasColumnMetadata) {
        try {
            String name = row.getString(CF_NAME);

            List<ColumnMetadata> partitionKey = new ArrayList<ColumnMetadata>();
            List<ColumnMetadata> clusteringKey = new ArrayList<ColumnMetadata>();
            // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
            LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();

            // First, figure out which kind of table we are
            boolean isCompact = false;
            AbstractType ct = TypeParser.parse(row.getString(COMPARATOR));
            boolean isComposite = ct instanceof CompositeType;
            List<AbstractType<?>> columnTypes = isComposite
                                              ? ((CompositeType)ct).types
                                              : Collections.<AbstractType<?>>singletonList(ct);
            List<String> columnAliases = fromJsonList(row.getString(COLUMN_ALIASES));
            int clusteringSize;
            boolean hasValue;
            int last = columnTypes.size() - 1;
            AbstractType lastType = columnTypes.get(last);
            if (isComposite) {
                if (lastType instanceof ColumnToCollectionType || (columnAliases.size() == last && lastType instanceof UTF8Type)) {
                    hasValue = false;
                    clusteringSize = lastType instanceof ColumnToCollectionType ? last - 1 : last;
                } else {
                    isCompact = true;
                    hasValue = true;
                    clusteringSize = columnTypes.size();
                }
            } else {
                isCompact = true;
                if (!columnAliases.isEmpty() || !hasColumnMetadata) {
                    hasValue = true;
                    clusteringSize = columnTypes.size();
                } else {
                    hasValue = false;
                    clusteringSize = 0;
                }
            }

            TableMetadata tm = new TableMetadata(ksm, name, partitionKey, clusteringKey, columns, new Options(row, isCompact));

            // Partition key
            AbstractType kt = TypeParser.parse(row.getString(KEY_VALIDATOR));
            List<AbstractType<?>> keyTypes = kt instanceof CompositeType
                                           ? ((CompositeType)kt).types
                                           : Collections.<AbstractType<?>>singletonList(kt);

            // check if key_aliases is null, and set to [] due to CASSANDRA-5101
            List<String> keyAliases = row.getString(KEY_ALIASES) == null ? Collections.<String>emptyList() : fromJsonList(row.getString(KEY_ALIASES));
            for (int i = 0; i < keyTypes.size(); i++) {
                String cn = keyAliases.size() > i
                          ? keyAliases.get(i)
                          : (i == 0 ? DEFAULT_KEY_ALIAS : DEFAULT_KEY_ALIAS + (i + 1));
                DataType dt = Codec.rawTypeToDataType(keyTypes.get(i));
                ColumnMetadata colMeta = new ColumnMetadata(tm, cn, dt, null);
                columns.put(cn, colMeta);
                partitionKey.add(colMeta);
            }

            // Clustering key
            for (int i = 0; i < clusteringSize; i++) {
                String cn = columnAliases.size() > i ? columnAliases.get(i) : DEFAULT_COLUMN_ALIAS + (i + 1);
                DataType dt = Codec.rawTypeToDataType(columnTypes.get(i));
                ColumnMetadata colMeta = new ColumnMetadata(tm, cn, dt, null);
                columns.put(cn, colMeta);
                clusteringKey.add(colMeta);
            }

            // Value alias (if present)
            if (hasValue) {
                AbstractType vt = TypeParser.parse(row.getString(VALIDATOR));
                String valueAlias = row.isNull(KEY_ALIASES) ? DEFAULT_VALUE_ALIAS : row.getString(VALUE_ALIAS);
                ColumnMetadata vm = new ColumnMetadata(tm, valueAlias, Codec.rawTypeToDataType(vt), null);
                columns.put(valueAlias, vm);
            }

            ksm.add(tm);
            return tm;
        } catch (RequestValidationException e) {
            // The server will have validated the type
            throw new RuntimeException(e);
        }
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
     * keys in their defined order, and then the rest of the
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
        List<ColumnMetadata> pk = new ArrayList<ColumnMetadata>(partitionKey.size() + clusteringKey.size());
        pk.addAll(partitionKey);
        pk.addAll(clusteringKey);
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
     * Returns the list of columns composing the clustering key for this table.
     *
     * @return the list of columns composing the clustering key for this table.
     * If the clustering key is empty, an empty list is returned.
     */
    public List<ColumnMetadata> getClusteringKey() {
        return Collections.unmodifiableList(clusteringKey);
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

    static List<String> fromJsonList(String json) {
        try {
            return jsonMapper.readValue(json, List.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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

    private String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(name).append(" (");
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
                sb.append(cm.getName());
            }
            sb.append(")");
        }
        for (ColumnMetadata cm : clusteringKey)
            sb.append(", ").append(cm.getName());
        sb.append(")");
        newLine(sb, formatted);
        // end PK

        // Options
        if (options.isCompactStorage) {
            sb.append(") WITH COMPACT STORAGE");
            and(sb, formatted).append("read_repair_chance = ").append(options.readRepair);
        } else {
            sb.append(") WITH read_repair_chance = ").append(options.readRepair);
        }
        and(sb, formatted).append("dclocal_read_repair_chance = ").append(options.localReadRepair);
        and(sb, formatted).append("replicate_on_write = ").append(options.replicateOnWrite);
        and(sb, formatted).append("gc_grace_seconds = ").append(options.gcGrace);
        and(sb, formatted).append("bloom_filter_fp_chance = ").append(options.bfFpChance);
        and(sb, formatted).append("caching = ").append(options.caching);
        if (options.comment != null)
            and(sb, formatted).append("comment = '").append(options.comment).append("'");
        and(sb, formatted).append("compaction = ").append(formatOptionMap(options.compaction));
        and(sb, formatted).append("compression = ").append(formatOptionMap(options.compression));
        sb.append(";");
        return sb.toString();
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
        return newLine(sb, formatted).append(spaces(3, formatted)).append("AND ");
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
        private static final String COMPRESSION_PARAMS       = "compression_parameters";

        private static final double DEFAULT_BF_FP_CHANCE = 0.01;

        private final boolean isCompactStorage;

        private final String comment;
        private final double readRepair;
        private final double localReadRepair;
        private final boolean replicateOnWrite;
        private final int gcGrace;
        private final double bfFpChance;
        private final String caching;
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
