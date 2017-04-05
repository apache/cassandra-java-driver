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


import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.*;

/**
 * Base class for Tables and Materialized Views metadata.
 */
public abstract class AbstractTableMetadata {

    static final Comparator<ColumnMetadata> columnMetadataComparator = new Comparator<ColumnMetadata>() {
        @Override
        public int compare(ColumnMetadata c1, ColumnMetadata c2) {
            return c1.getName().compareTo(c2.getName());
        }
    };

    static final Predicate<ClusteringOrder> isAscending = new Predicate<ClusteringOrder>() {
        @Override
        public boolean apply(ClusteringOrder o) {
            return o == ClusteringOrder.ASC;
        }
    };

    protected final KeyspaceMetadata keyspace;
    protected final String name;
    protected final UUID id;
    protected final List<ColumnMetadata> partitionKey;
    protected final List<ColumnMetadata> clusteringColumns;
    protected final Map<String, ColumnMetadata> columns;
    protected final TableOptionsMetadata options;
    protected final List<ClusteringOrder> clusteringOrder;
    protected final VersionNumber cassandraVersion;

    protected AbstractTableMetadata(KeyspaceMetadata keyspace,
                                    String name,
                                    UUID id,
                                    List<ColumnMetadata> partitionKey,
                                    List<ColumnMetadata> clusteringColumns,
                                    Map<String, ColumnMetadata> columns,
                                    TableOptionsMetadata options,
                                    List<ClusteringOrder> clusteringOrder,
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
     * <p/>
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
     *             interpreted as a case-insensitive identifier unless enclosed in double-quotes,
     *             see {@link Metadata#quote}).
     * @return the metadata for the column if it exists, or
     * {@code null} otherwise.
     */
    public ColumnMetadata getColumn(String name) {
        return columns.get(Metadata.handleId(name));
    }

    /**
     * Returns a list containing all the columns of this table.
     * <p/>
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
     * <p/>
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
     * <p/>
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
     * <p/>
     * The returned contains the clustering order of each clustering column. The
     * {@code i}th element of the result correspond to the order (ascending or
     * descending) of the {@code i}th clustering column (see
     * {@link #getClusteringColumns}). Note that a table defined without any
     * particular clustering order is equivalent to one for which all the
     * clustering keys are in ascending order.
     *
     * @return a list with the clustering order for each clustering column.
     */
    public List<ClusteringOrder> getClusteringOrder() {
        return clusteringOrder;
    }

    /**
     * Returns the options for this table.
     *
     * @return the options for this table.
     */
    public TableOptionsMetadata getOptions() {
        return options;
    }

    void add(ColumnMetadata column) {
        columns.put(column.getName(), column);
    }

    /**
     * Returns a {@code String} containing CQL queries representing this
     * table and the index on it.
     * <p/>
     * In other words, this method returns the queries that would allow you to
     * recreate the schema of this table, along with the indexes and views defined on
     * this table, if any.
     * <p/>
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
     * <p/>
     * This method returns a single 'CREATE TABLE' query with the options
     * corresponding to this table definition.
     * <p/>
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

    protected StringBuilder appendOptions(StringBuilder sb, boolean formatted) {
        // Options
        sb.append(" WITH ");
        if (options.isCompactStorage())
            and(sb.append("COMPACT STORAGE"), formatted);
        if (!Iterables.all(clusteringOrder, isAscending))
            and(appendClusteringOrder(sb), formatted);
        sb.append("read_repair_chance = ").append(options.getReadRepairChance());
        and(sb, formatted).append("dclocal_read_repair_chance = ").append(options.getLocalReadRepairChance());
        if (cassandraVersion.getMajor() < 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() == 0))
            and(sb, formatted).append("replicate_on_write = ").append(options.getReplicateOnWrite());
        and(sb, formatted).append("gc_grace_seconds = ").append(options.getGcGraceInSeconds());
        and(sb, formatted).append("bloom_filter_fp_chance = ").append(options.getBloomFilterFalsePositiveChance());
        if (cassandraVersion.getMajor() < 2 || cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() < 1)
            and(sb, formatted).append("caching = '").append(options.getCaching().get("keys")).append('\'');
        else
            and(sb, formatted).append("caching = ").append(formatOptionMap(options.getCaching()));
        if (options.getComment() != null)
            and(sb, formatted).append("comment = '").append(options.getComment().replace("'", "''")).append('\'');
        and(sb, formatted).append("compaction = ").append(formatOptionMap(options.getCompaction()));
        and(sb, formatted).append("compression = ").append(formatOptionMap(options.getCompression()));
        if (cassandraVersion.getMajor() >= 2) {
            and(sb, formatted).append("default_time_to_live = ").append(options.getDefaultTimeToLive());
            and(sb, formatted).append("speculative_retry = '").append(options.getSpeculativeRetry()).append('\'');
            if (options.getIndexInterval() != null)
                and(sb, formatted).append("index_interval = ").append(options.getIndexInterval());
        }
        if (cassandraVersion.getMajor() > 2 || (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1)) {
            and(sb, formatted).append("min_index_interval = ").append(options.getMinIndexInterval());
            and(sb, formatted).append("max_index_interval = ").append(options.getMaxIndexInterval());
        }
        if (cassandraVersion.getMajor() > 2) {
            and(sb, formatted).append("crc_check_chance = ").append(options.getCrcCheckChance());
        }
        if (cassandraVersion.getMajor() > 3 || (cassandraVersion.getMajor() == 3 && cassandraVersion.getMinor() >= 8)) {
            and(sb, formatted).append("cdc = ").append(options.isCDC());
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
            if (first) first = false;
            else sb.append(", ");
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

}
