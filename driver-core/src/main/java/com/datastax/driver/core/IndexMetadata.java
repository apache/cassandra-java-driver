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

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * An immutable representation of secondary index metadata.
 */
public class IndexMetadata {

    public enum IndexType {
        KEYS,
        CUSTOM,
        COMPOSITES
    }

    public enum TargetType {
        COLUMN, ROW
    }

    public static final Function<IndexMetadata, String> INDEX_NAME = new Function<IndexMetadata, String>() {
        @Override
        public String apply(IndexMetadata input) {
            return input.getName();
        }
    };

    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * The name of the option used to specify that the index is on the collection keys.
     */
    public static final String INDEX_KEYS_OPTION_NAME = "index_keys";

    /**
     * The name of the option used to specify that the index is on the collection values.
     */
    public static final String INDEX_VALUES_OPTION_NAME = "index_values";

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    public static final String INDEX_ENTRIES_OPTION_NAME = "index_keys_and_values";


    private final TableMetadata table;
    private final String name;
    private final Map<String, ColumnMetadata> columns;
    private final IndexType indexType;
    private final TargetType targetType;
    private final Map<String, String> options;

    private IndexMetadata(TableMetadata table, String name, Map<String, ColumnMetadata> columns, IndexType indexType, TargetType targetType, Map<String, String> options) {
        this.table = table;
        this.name = name;
        this.columns = columns;
        this.indexType = indexType;
        this.targetType = targetType;
        this.options = options;
    }

    /**
     * Build an IndexMetadata from a system_schema.indexes row.
     */
    static IndexMetadata fromRow(TableMetadata table, Row indexRow) {
        String name = indexRow.getString("index_name");
        Set<String> targetColumnNames = indexRow.getSet("target_columns", String.class);
        LinkedHashMap<String, ColumnMetadata> targetColumns = new LinkedHashMap<String, ColumnMetadata>(targetColumnNames.size());
        for (String targetColumnName : targetColumnNames) {
            targetColumns.put(targetColumnName, table.getColumn(targetColumnName));
        }
        IndexMetadata.IndexType indexType = IndexMetadata.IndexType.valueOf(indexRow.getString("index_type"));
        IndexMetadata.TargetType targetType = IndexMetadata.TargetType.valueOf(indexRow.getString("target_type"));
        Map<String, String> options = indexRow.getMap("options", String.class, String.class);
        return new IndexMetadata(table, name, targetColumns, indexType, targetType, options);
    }

    /**
     * Build an IndexMetadata from a legacy layout (index information is stored
     * along with indexed column).
     */
    static IndexMetadata fromLegacy(ColumnMetadata column, ColumnMetadata.Raw raw) {
        if (raw.indexColumns.isEmpty())
            return null;
        String type = raw.indexColumns.get(ColumnMetadata.INDEX_TYPE);
        if (type == null)
            return null;
        String indexName = raw.indexColumns.get(ColumnMetadata.INDEX_NAME);
        String indexTypeStr = raw.indexColumns.get(ColumnMetadata.INDEX_TYPE);
        IndexType indexType = indexTypeStr == null ? null : IndexType.valueOf(indexTypeStr);
        // Special case check for the value of the index_options column being a string with value 'null' as this
        // column appears to be set this way (JAVA-834).
        String indexOptionsCol = raw.indexColumns.get(ColumnMetadata.INDEX_OPTIONS);
        ImmutableMap<String, ColumnMetadata> columns = ImmutableMap.of(column.getName(), column);
        Map<String, String> options;
        if (indexOptionsCol == null || indexOptionsCol.isEmpty() || indexOptionsCol.equals("null")) {
            options = ImmutableMap.of();
        } else {
            options = SimpleJSONParser.parseStringMap(indexOptionsCol);
        }
        return new IndexMetadata((TableMetadata)column.getParent(), indexName, columns, indexType, TargetType.COLUMN, options);
    }

    /**
     * Returns the metadata of the table this column is part of.
     *
     * @return the {@code TableMetadata} for the table this column is part of.
     */
    public TableMetadata getTable() {
        return table;
    }

    /**
     * Returns the index name.
     *
     * @return the index name.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns metadata on a column of this index.
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
     * Returns a list containing all the columns of this index.
     *
     * The order of the columns in the list is consistent with
     * the order of the columns in the index.
     *
     * @return a list containing the metadata for the columns of this table.
     */
    public List<ColumnMetadata> getColumns() {
        return new ArrayList<ColumnMetadata>(columns.values());
    }

    /**
     * Returns the index type.
     *
     * @return the index type.
     */
    public IndexType getIndexType() {
        return indexType;
    }

    /**
     * Returns the index target type.
     * Note: for legacy indexes, this is always {@link TargetType#COLUMN}.
     *
     * @return the index target type.
     */
    public TargetType getTargetType() {
        return targetType;
    }

    /**
     * Returns whether this index is a custom one.
     * <p>
     * If it is indeed a custom index, {@link #getIndexClassName} will
     * return the name of the class used in Cassandra to implement that
     * index.
     *
     * @return {@code true} if this metadata represents a custom index.
     */
    public boolean isCustomIndex() {
        return getIndexClassName() != null;
    }

    /**
     * The name of the class used to implement the custom index, if it is one.
     *
     * @return the name of the class used Cassandra side to implement this
     * custom index if {@code isCustomIndex() == true}, {@code null} otherwise.
     */
    public String getIndexClassName() {
        return getOption(CUSTOM_INDEX_OPTION_NAME);
    }

    /**
     * Return whether this index is a 'KEYS' index on a map, e.g.,
     * CREATE INDEX ON mytable (KEYS(mymap))
     *
     * @return {@code true} if this is a 'KEYS' index on a map.
     */
    public boolean isKeys() {
        return getOption(INDEX_KEYS_OPTION_NAME) != null;
    }

    /**
     * Return whether this index is a 'VALUES' index on a map, e.g.,
     * CREATE INDEX ON mytable (VALUES(mymap))
     *
     * @return {@code true} if this is an 'VALUES' index on a map.
     */
    public boolean isValues() {
        return getOption(INDEX_VALUES_OPTION_NAME) != null;
    }

    /**
     * Return whether this index is a 'ENTRIES' index on a map, e.g.,
     * CREATE INDEX ON mytable (ENTRIES(mymap))
     *
     * @return {@code true} if this is an 'ENTRIES' index on a map.
     */
    public boolean isEntries() {
        return getOption(INDEX_ENTRIES_OPTION_NAME) != null;
    }

    /**
     * Return whether this index is a 'FULL' index on a frozen collection, e.g.,
     * CREATE INDEX ON mytable (FULL(mymap))
     *
     * @return {@code true} if this is a 'FULL' index on a frozen collection.
     */
    public boolean isFull() {
        /*
         * This check is analogous to the Cassandra counterpart
         * in IndexTarget.
         */
        ColumnMetadata column = columns.values().iterator().next();
        return
            !isKeys()
            && !isValues()
            && !isEntries()
            && column.getType() instanceof DataType.CollectionType
            && column.getType().isFrozen();
    }

    /**
     * Return the value for the given option name.
     *
     * @param name Option name
     * @return Option value
     */
    public String getOption(String name) {
        return options != null ? options.get(name) : null;
    }

    /**
     * Returns a CQL query representing this index.
     *
     * This method returns a single 'CREATE INDEX' query corresponding to
     * this index definition.
     *
     * @return the 'CREATE INDEX' query corresponding to this index.
     */
    public String asCQLQuery() {
        TableMetadata table = getTable();
        String ksName = Metadata.escapeId(table.getKeyspace().getName());
        String cfName = Metadata.escapeId(table.getName());
        // TODO indexes on multiple columns
        String colName = Metadata.escapeId(columns.keySet().iterator().next());
        return isCustomIndex()
            ? String.format("CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s' WITH OPTIONS = %s;", name, ksName, cfName, colName, getIndexClassName(), getOptionsAsCql())
            : String.format("CREATE INDEX %s ON %s.%s (%s);", name, ksName, cfName, getIndexFunction(colName));
    }

    /**
     * Builds a string representation of the custom index options.
     *
     * @return String representation of the custom index options, similar to what Cassandra stores in
     *         the 'index_options' column of the 'schema_columns' table in the 'system' keyspace.
     */
    private String getOptionsAsCql() {
        StringBuilder builder = new StringBuilder();
        builder.append("{");
        Iterator<Map.Entry<String, String>> it = options.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> option = it.next();
            builder.append(String.format("'%s' : '%s'", option.getKey(), option.getValue()));
            if (it.hasNext())
                builder.append(", ");
        }
        builder.append("}");
        return builder.toString();
    }

    /**
     * Wraps the column name with the appropriate index function (KEYS, FULL, ENTRIES),
     * if necessary.
     *
     * @return Column name wrapped with the appropriate index function.
     */
    private String getIndexFunction(String colName) {
        if (isKeys())
            return String.format("KEYS(%s)", colName);
        else if (isFull())
            return String.format("FULL(%s)", colName);
        else if (isEntries())
            return String.format("ENTRIES(%s)", colName);
        return colName;
    }

    public int hashCode() {
        return Objects.hashCode(name, columns, indexType, targetType, options);
    }

    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexMetadata))
            return false;

        IndexMetadata other = (IndexMetadata)obj;

        return Objects.equal(name, other.name)
            && Objects.equal(columns, other.columns)
            && Objects.equal(indexType, other.indexType)
            && Objects.equal(targetType, other.targetType)
            && Objects.equal(options, other.options);
    }

}
