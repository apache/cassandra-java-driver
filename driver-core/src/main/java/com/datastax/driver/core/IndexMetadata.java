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

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.Map;

/**
 * An immutable representation of secondary index metadata.
 */
public class IndexMetadata {

    public enum Kind {
        KEYS,
        CUSTOM,
        COMPOSITES
    }

    static final String NAME = "index_name";

    static final String KIND = "kind";

    static final String OPTIONS = "options";

    /**
     * The name of the option used to specify the index target (Cassandra 3.0 onwards).
     */
    public static final String TARGET_OPTION_NAME = "target";

    /**
     * The name of the option used to specify a custom index class name.
     */
    public static final String CUSTOM_INDEX_OPTION_NAME = "class_name";

    /**
     * The name of the option used to specify that the index is on the collection (map) keys.
     */
    public static final String INDEX_KEYS_OPTION_NAME = "index_keys";

    /**
     * The name of the option used to specify that the index is on the collection (map) entries.
     */
    public static final String INDEX_ENTRIES_OPTION_NAME = "index_keys_and_values";

    private final TableMetadata table;
    private final String name;
    private final Kind kind;
    private final String target;
    private final Map<String, String> options;

    private IndexMetadata(TableMetadata table, String name, Kind kind, String target, Map<String, String> options) {
        this.table = table;
        this.name = name;
        this.kind = kind;
        this.target = target;
        this.options = options;
    }

    /**
     * Build an IndexMetadata from a system_schema.indexes row.
     */
    static IndexMetadata fromRow(TableMetadata table, Row indexRow) {
        String name = indexRow.getString(NAME);
        Kind kind = Kind.valueOf(indexRow.getString(KIND));
        Map<String, String> options = indexRow.getMap(OPTIONS, String.class, String.class);
        String target = options.get(TARGET_OPTION_NAME);
        return new IndexMetadata(table, name, kind, target, options);
    }

    /**
     * Build an IndexMetadata from a legacy layout (index information is stored
     * along with indexed column).
     */
    static IndexMetadata fromLegacy(ColumnMetadata column, ColumnMetadata.Raw raw) {
        Map<String, String> indexColumns = raw.indexColumns;
        if (indexColumns.isEmpty())
            return null;
        String type = indexColumns.get(ColumnMetadata.INDEX_TYPE);
        if (type == null)
            return null;
        String indexName = indexColumns.get(ColumnMetadata.INDEX_NAME);
        String kindStr = indexColumns.get(ColumnMetadata.INDEX_TYPE);
        Kind kind = kindStr == null ? null : Kind.valueOf(kindStr);
        // Special case check for the value of the index_options column being a string with value 'null' as this
        // column appears to be set this way (JAVA-834).
        String indexOptionsCol = indexColumns.get(ColumnMetadata.INDEX_OPTIONS);
        Map<String, String> options;
        if (indexOptionsCol == null || indexOptionsCol.isEmpty() || indexOptionsCol.equals("null")) {
            options = ImmutableMap.of();
        } else {
            options = SimpleJSONParser.parseStringMap(indexOptionsCol);
        }
        String target = targetFromLegacyOptions(column, options);
        return new IndexMetadata((TableMetadata) column.getParent(), indexName, kind, target, options);
    }

    private static String targetFromLegacyOptions(ColumnMetadata column, Map<String, String> options) {
        String columnName = Metadata.escapeId(column.getName());
        if (options.containsKey(INDEX_KEYS_OPTION_NAME))
            return String.format("keys(%s)", columnName);
        if (options.containsKey(INDEX_ENTRIES_OPTION_NAME))
            return String.format("entries(%s)", columnName);
        if (column.getType() instanceof DataType.CollectionType && column.getType().isFrozen())
            return String.format("full(%s)", columnName);
        // Note: the keyword 'values' is not accepted as a valid index target function until 3.0
        return columnName;
    }

    /**
     * Returns the metadata of the table this index is part of.
     *
     * @return the table this index is part of.
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
     * Returns the index kind.
     *
     * @return the index kind.
     */
    public Kind getKind() {
        return kind;
    }

    /**
     * Returns the index target.
     *
     * @return the index target.
     */
    public String getTarget() {
        return target;
    }

    /**
     * Returns whether this index is a custom one.
     * <p/>
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
     * <p/>
     * This method returns a single 'CREATE INDEX' query corresponding to
     * this index definition.
     *
     * @return the 'CREATE INDEX' query corresponding to this index.
     */
    public String asCQLQuery() {
        String keyspaceName = Metadata.escapeId(table.getKeyspace().getName());
        String tableName = Metadata.escapeId(table.getName());
        String indexName = Metadata.escapeId(this.name);
        return isCustomIndex()
                ? String.format("CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s' %s;", indexName, keyspaceName, tableName, getTarget(), getIndexClassName(), getOptionsAsCql())
                : String.format("CREATE INDEX %s ON %s.%s (%s);", indexName, keyspaceName, tableName, getTarget());
    }

    /**
     * Builds a string representation of the custom index options.
     *
     * @return String representation of the custom index options, similar to what Cassandra stores in
     * the 'index_options' column of the 'schema_columns' table in the 'system' keyspace.
     */
    private String getOptionsAsCql() {
        Iterable<Map.Entry<String, String>> filtered = Iterables.filter(options.entrySet(), new Predicate<Map.Entry<String, String>>() {
            @Override
            public boolean apply(Map.Entry<String, String> input) {
                return
                        !input.getKey().equals(TARGET_OPTION_NAME) &&
                                !input.getKey().equals(CUSTOM_INDEX_OPTION_NAME);
            }
        });
        if (Iterables.isEmpty(filtered)) return "";
        StringBuilder builder = new StringBuilder();
        builder.append("WITH OPTIONS = {");
        Iterator<Map.Entry<String, String>> it = filtered.iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> option = it.next();
            builder.append(String.format("'%s' : '%s'", option.getKey(), option.getValue()));
            if (it.hasNext())
                builder.append(", ");
        }
        builder.append("}");
        return builder.toString();
    }

    public int hashCode() {
        return Objects.hashCode(name, kind, target, options);
    }

    public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof IndexMetadata))
            return false;

        IndexMetadata other = (IndexMetadata) obj;

        return Objects.equal(name, other.name)
                && Objects.equal(kind, other.kind)
                && Objects.equal(target, other.target)
                && Objects.equal(options, other.options);
    }

}
