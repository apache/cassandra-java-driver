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

import com.datastax.driver.core.utils.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Describes a Table.
 */
public class TableMetadata extends AbstractTableMetadata {

    private static final Logger logger = LoggerFactory.getLogger(TableMetadata.class);

    private static final String CF_ID_V2 = "cf_id";
    private static final String CF_ID_V3 = "id";

    private static final String KEY_VALIDATOR = "key_validator";
    private static final String COMPARATOR = "comparator";
    private static final String VALIDATOR = "default_validator";

    private static final String KEY_ALIASES = "key_aliases";
    private static final String COLUMN_ALIASES = "column_aliases";
    private static final String VALUE_ALIAS = "value_alias";

    private static final String DEFAULT_KEY_ALIAS = "key";
    private static final String DEFAULT_COLUMN_ALIAS = "column";
    private static final String DEFAULT_VALUE_ALIAS = "value";

    private static final String FLAGS = "flags";
    private static final String DENSE = "dense";
    private static final String SUPER = "super";
    private static final String COMPOUND = "compound";

    private static final String EMPTY_TYPE = "empty";

    private final Map<String, IndexMetadata> indexes;

    private final Map<String, MaterializedViewMetadata> views;

    private TableMetadata(KeyspaceMetadata keyspace,
                          String name,
                          UUID id,
                          List<ColumnMetadata> partitionKey,
                          List<ColumnMetadata> clusteringColumns,
                          Map<String, ColumnMetadata> columns,
                          Map<String, IndexMetadata> indexes,
                          TableOptionsMetadata options,
                          List<ClusteringOrder> clusteringOrder,
                          VersionNumber cassandraVersion) {
        super(keyspace, name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, cassandraVersion);
        this.indexes = indexes;
        this.views = new HashMap<String, MaterializedViewMetadata>();
    }

    static TableMetadata build(KeyspaceMetadata ksm, Row row, Map<String, ColumnMetadata.Raw> rawCols, List<Row> indexRows, String nameColumn, VersionNumber cassandraVersion, Cluster cluster) {

        String name = row.getString(nameColumn);

        UUID id = null;

        if (cassandraVersion.getMajor() == 2 && cassandraVersion.getMinor() >= 1)
            id = row.getUUID(CF_ID_V2);
        else if (cassandraVersion.getMajor() > 2)
            id = row.getUUID(CF_ID_V3);

        DataTypeClassNameParser.ParseResult comparator = null;
        DataTypeClassNameParser.ParseResult keyValidator = null;
        List<String> columnAliases = null;

        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = cluster.getConfiguration().getCodecRegistry();

        if (cassandraVersion.getMajor() <= 2) {
            comparator = DataTypeClassNameParser.parseWithComposite(row.getString(COMPARATOR), protocolVersion, codecRegistry);
            keyValidator = DataTypeClassNameParser.parseWithComposite(row.getString(KEY_VALIDATOR), protocolVersion, codecRegistry);
            columnAliases = cassandraVersion.getMajor() >= 2 || row.getString(COLUMN_ALIASES) == null
                    ? Collections.<String>emptyList()
                    : SimpleJSONParser.parseStringList(row.getString(COLUMN_ALIASES));
        }

        int partitionKeySize = findPartitionKeySize(rawCols.values(), keyValidator);
        int clusteringSize;

        boolean isDense;
        boolean isCompact;
        if (cassandraVersion.getMajor() > 2) {
            Set<String> flags = row.getSet(FLAGS, String.class);
            isDense = flags.contains(DENSE);
            boolean isSuper = flags.contains(SUPER);
            boolean isCompound = flags.contains(COMPOUND);
            isCompact = isSuper || isDense || !isCompound;
            boolean isStaticCompact = !isSuper && !isDense && !isCompound;
            if (isStaticCompact) {
                rawCols = pruneStaticCompactTableColumns(rawCols);
            }
            if (isDense) {
                rawCols = pruneDenseTableColumnsV3(rawCols);
            }
            clusteringSize = findClusteringSize(comparator, rawCols.values(), columnAliases, cassandraVersion);
        } else {
            assert comparator != null;
            clusteringSize = findClusteringSize(comparator, rawCols.values(), columnAliases, cassandraVersion);
            isDense = clusteringSize != comparator.types.size() - 1;
            if (isDense) {
                rawCols = pruneDenseTableColumnsV2(rawCols);
            }
            isCompact = isDense || !comparator.isComposite;
        }

        List<ColumnMetadata> partitionKey = new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(partitionKeySize, null));
        List<ColumnMetadata> clusteringColumns = new ArrayList<ColumnMetadata>(Collections.<ColumnMetadata>nCopies(clusteringSize, null));
        List<ClusteringOrder> clusteringOrder = new ArrayList<ClusteringOrder>(Collections.<ClusteringOrder>nCopies(clusteringSize, null));

        // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
        LinkedHashMap<String, ColumnMetadata> columns = new LinkedHashMap<String, ColumnMetadata>();
        LinkedHashMap<String, IndexMetadata> indexes = new LinkedHashMap<String, IndexMetadata>();

        TableOptionsMetadata options = null;
        try {
            options = new TableOptionsMetadata(row, isCompact, cassandraVersion);
        } catch (RuntimeException e) {
            // See ControlConnection#refreshSchema for why we'd rather not probably this further. Since table options is one thing
            // that tends to change often in Cassandra, it's worth special casing this.
            logger.error(String.format("Error parsing schema options for table %s.%s: "
                            + "Cluster.getMetadata().getKeyspace(\"%s\").getTable(\"%s\").getOptions() will return null",
                    ksm.getName(), name, ksm.getName(), name), e);
        }

        TableMetadata tm = new TableMetadata(ksm, name, id, partitionKey, clusteringColumns, columns, indexes, options, clusteringOrder, cassandraVersion);

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
                clusteringOrder.set(i, comparator.reversed.get(i) ? ClusteringOrder.DESC : ClusteringOrder.ASC);
            }

            // if we're dense, chances are that we have a single regular "value" column with an alias
            if (isDense) {
                String alias = row.isNull(VALUE_ALIAS) ? DEFAULT_VALUE_ALIAS : row.getString(VALUE_ALIAS);
                // ...unless the table does not have any regular column, only primary key columns (JAVA-873)
                if (!alias.isEmpty()) {
                    DataType type = DataTypeClassNameParser.parseOne(row.getString(VALIDATOR), protocolVersion, codecRegistry);
                    otherColumns.add(ColumnMetadata.forAlias(tm, alias, type));
                }
            }
        }

        for (ColumnMetadata.Raw rawCol : rawCols.values()) {
            DataType dataType;
            if (cassandraVersion.getMajor() >= 3) {
                dataType = DataTypeCqlNameParser.parse(rawCol.dataType, cluster, ksm.getName(), ksm.userTypes, null, false, false);
            } else {
                dataType = DataTypeClassNameParser.parseOne(rawCol.dataType, protocolVersion, codecRegistry);
            }
            ColumnMetadata col = ColumnMetadata.fromRaw(tm, rawCol, dataType);
            switch (rawCol.kind) {
                case PARTITION_KEY:
                    partitionKey.set(rawCol.position, col);
                    break;
                case CLUSTERING_COLUMN:
                    clusteringColumns.set(rawCol.position, col);
                    clusteringOrder.set(rawCol.position, rawCol.isReversed ? ClusteringOrder.DESC : ClusteringOrder.ASC);
                    break;
                default:
                    otherColumns.add(col);
                    break;
            }

            // legacy secondary indexes (C* < 3.0)
            IndexMetadata index = IndexMetadata.fromLegacy(col, rawCol);
            if (index != null)
                indexes.put(index.getName(), index);
        }

        for (ColumnMetadata c : partitionKey)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : clusteringColumns)
            columns.put(c.getName(), c);
        for (ColumnMetadata c : otherColumns)
            columns.put(c.getName(), c);

        // create secondary indexes (C* >= 3.0)
        if (indexRows != null)
            for (Row indexRow : indexRows) {
                IndexMetadata index = IndexMetadata.fromRow(tm, indexRow);
                indexes.put(index.getName(), index);
            }

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
        while (it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if (col.kind == ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN) {
                // remove "column1 text" clustering column
                it.remove();
            } else if (col.kind == ColumnMetadata.Raw.Kind.REGULAR) {
                // remove "value blob" regular column
                it.remove();
            } else if (col.kind == ColumnMetadata.Raw.Kind.STATIC) {
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
        while (it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if (col.kind == ColumnMetadata.Raw.Kind.REGULAR
                    && col.dataType.equals(EMPTY_TYPE)) {
                // remove "value empty" regular column
                it.remove();
            }
        }
        return rawCols;
    }

    private static Map<String, ColumnMetadata.Raw> pruneDenseTableColumnsV2(Map<String, ColumnMetadata.Raw> rawCols) {
        Collection<ColumnMetadata.Raw> cols = rawCols.values();
        Iterator<ColumnMetadata.Raw> it = cols.iterator();
        while (it.hasNext()) {
            ColumnMetadata.Raw col = it.next();
            if (col.kind == ColumnMetadata.Raw.Kind.COMPACT_VALUE && col.name.isEmpty()) {
                // remove "" blob regular COMPACT_VALUE column
                it.remove();
            }
        }
        return rawCols;
    }

    private static int findPartitionKeySize(Collection<ColumnMetadata.Raw> cols, DataTypeClassNameParser.ParseResult keyValidator) {
        // C* 1.2, 2.0, 2.1 and 2.2
        if (keyValidator != null)
            return keyValidator.types.size();
        // C* 3.0 onwards
        int maxId = -1;
        for (ColumnMetadata.Raw col : cols)
            if (col.kind == ColumnMetadata.Raw.Kind.PARTITION_KEY)
                maxId = Math.max(maxId, col.position);
        return maxId + 1;
    }

    private static int findClusteringSize(DataTypeClassNameParser.ParseResult comparator,
                                          Collection<ColumnMetadata.Raw> cols,
                                          List<String> columnAliases,
                                          VersionNumber cassandraVersion) {
        // In 2.0 onwards, this is relatively easy, we just find the biggest 'position' amongst the clustering columns.
        // For 1.2 however, this is slightly more subtle: we need to infer it based on whether the comparator is composite or not, and whether we have
        // regular columns or not.
        if (cassandraVersion.getMajor() >= 2) {
            int maxId = -1;
            for (ColumnMetadata.Raw col : cols)
                if (col.kind == ColumnMetadata.Raw.Kind.CLUSTERING_COLUMN)
                    maxId = Math.max(maxId, col.position);
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

    /**
     * Returns metadata on a index of this table.
     *
     * @param name the name of the index to retrieve ({@code name} will be
     *             interpreted as a case-insensitive identifier unless enclosed in double-quotes,
     *             see {@link Metadata#quote}).
     * @return the metadata for the {@code name} index if it exists, or
     * {@code null} otherwise.
     */
    public IndexMetadata getIndex(String name) {
        return indexes.get(Metadata.handleId(name));
    }

    /**
     * Returns all indexes based on this table.
     *
     * @return all indexes based on this table.
     */
    public Collection<IndexMetadata> getIndexes() {
        return Collections.unmodifiableCollection(indexes.values());
    }

    /**
     * Returns metadata on a view of this table.
     *
     * @param name the name of the view to retrieve ({@code name} will be
     *             interpreted as a case-insensitive identifier unless enclosed in double-quotes,
     *             see {@link Metadata#quote}).
     * @return the metadata for the {@code name} view if it exists, or
     * {@code null} otherwise.
     */
    public MaterializedViewMetadata getView(String name) {
        return views.get(Metadata.handleId(name));
    }

    /**
     * Returns all views based on this table.
     *
     * @return all views based on this table.
     */
    public Collection<MaterializedViewMetadata> getViews() {
        return Collections.unmodifiableCollection(views.values());
    }

    void add(MaterializedViewMetadata view) {
        views.put(view.getName(), view);
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
    @Override
    public String exportAsString() {
        StringBuilder sb = new StringBuilder();

        sb.append(super.exportAsString());

        for (IndexMetadata index : indexes.values()) {
            sb.append('\n').append(index.asCQLQuery());
        }

        for (MaterializedViewMetadata view : views.values()) {
            sb.append('\n').append(view.asCQLQuery());
        }

        return sb.toString();
    }

    @Override
    protected String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(Metadata.quoteIfNecessary(keyspace.getName())).append('.').append(Metadata.quoteIfNecessary(name)).append(" (");
        newLine(sb, formatted);
        for (ColumnMetadata cm : columns.values())
            newLine(sb.append(spaces(4, formatted)).append(cm).append(',').append(spaces(1, !formatted)), formatted);

        // PK
        sb.append(spaces(4, formatted)).append("PRIMARY KEY (");
        if (partitionKey.size() == 1) {
            sb.append(Metadata.quoteIfNecessary(partitionKey.get(0).getName()));
        } else {
            sb.append('(');
            boolean first = true;
            for (ColumnMetadata cm : partitionKey) {
                if (first)
                    first = false;
                else
                    sb.append(", ");
                sb.append(Metadata.quoteIfNecessary(cm.getName()));
            }
            sb.append(')');
        }
        for (ColumnMetadata cm : clusteringColumns)
            sb.append(", ").append(Metadata.quoteIfNecessary(cm.getName()));
        sb.append(')');
        newLine(sb, formatted);
        // end PK

        sb.append(")");
        appendOptions(sb, formatted);
        return sb.toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof TableMetadata))
            return false;

        TableMetadata that = (TableMetadata) other;

        return MoreObjects.equal(this.name, that.name) &&
                MoreObjects.equal(this.id, that.id) &&
                MoreObjects.equal(this.partitionKey, that.partitionKey) &&
                MoreObjects.equal(this.clusteringColumns, that.clusteringColumns) &&
                MoreObjects.equal(this.columns, that.columns) &&
                MoreObjects.equal(this.options, that.options) &&
                MoreObjects.equal(this.clusteringOrder, that.clusteringOrder) &&
                MoreObjects.equal(this.indexes, that.indexes) &&
                MoreObjects.equal(this.views, that.views);
    }

    @Override
    public int hashCode() {
        return MoreObjects.hashCode(name, id, partitionKey, clusteringColumns, columns, options, clusteringOrder, indexes, views);
    }
}
