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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Objects;

/**
 * Describes a Column.
 */
public class ColumnMetadata {

    static final String COLUMN_NAME = "column_name";
    static final String VALIDATOR = "validator";
    static final String COMPONENT_INDEX = "component_index";
    static final String KIND = "type";

    static final String INDEX_TYPE = "index_type";
    static final String INDEX_OPTIONS = "index_options";
    static final String INDEX_NAME = "index_name";
    private static final String CUSTOM_INDEX_CLASS = "class_name";

    private static final String INDEX_MAP_KEYS = "index_keys";
    private static final String INDEX_MAP_ENTRIES = "index_keys_and_values";
    private final TableMetadata table;
    private final String name;
    private final DataType type;
    private final IndexMetadata index;
    private final boolean isStatic;

    private ColumnMetadata(TableMetadata table, String name, DataType type, boolean isStatic, Map<String, String> indexColumns) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.isStatic = isStatic;
        this.index = IndexMetadata.build(this, indexColumns);
    }

    static ColumnMetadata fromRaw(TableMetadata tm, Raw raw) {
        return new ColumnMetadata(tm, raw.name, raw.dataType, raw.kind == Raw.Kind.STATIC, raw.indexColumns);
    }

    static ColumnMetadata forAlias(TableMetadata tm, String name, DataType type) {
        return new ColumnMetadata(tm, name, type, false, Collections.<String, String>emptyMap());
    }

    /**
     * Returns the name of the column.
     *
     * @return the name of the column.
     */
    public String getName() {
        return name;
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
     * Returns the type of the column.
     *
     * @return the type of the column.
     */
    public DataType getType() {
        return type;
    }

    /**
     * Returns the indexing metadata on this column if the column is indexed.
     *
     * @return the metadata on the column index if the column is indexed,
     * {@code null} otherwise.
     */
    public IndexMetadata getIndex() {
        return index;
    }

    /**
     * Whether this column is a static column.
     *
     * @return Whether this column is a static column or not.
     */
    public boolean isStatic() {
        return isStatic;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ColumnMetadata that = (ColumnMetadata)o;

        if (isStatic != that.isStatic)
            return false;
        if (!name.equals(that.name))
            return false;
        if (!type.equals(that.type))
            return false;
        return !(index != null ? !index.equals(that.index) : that.index != null);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (index != null ? index.hashCode() : 0);
        result = 31 * result + (isStatic ? 1 : 0);
        return result;
    }

    /**
     * Metadata on a column index.
     */
    public static class IndexMetadata {

        private final ColumnMetadata column;
        private final String name;
        private final Map<String, String> indexOptions;

        private IndexMetadata(ColumnMetadata column, String name) {
            this(column, name, null);
        }

        private IndexMetadata(ColumnMetadata column, String name, Map<String, String> indexOptions) {
            this.column = column;
            this.name = name;
            this.indexOptions = indexOptions;
        }

        /**
         * Returns the column this index metadata refers to.
         *
         * @return the column this index metadata refers to.
         */
        public ColumnMetadata getIndexedColumn() {
            return column;
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
            return getOption(CUSTOM_INDEX_CLASS);
        }

        /**
         * Return whether this index is a 'KEYS' index on a map, e.g.,
         * CREATE INDEX ON mytable (KEYS(mymap))
         * 
         * @return {@code true} if this is a 'KEYS' index on a map.
         */
        public boolean isKeys() {
            return getOption(INDEX_MAP_KEYS) != null;
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
             * in IndexTarget#fromColumnDefinition.
             */
            return !isKeys()
                && !isEntries()
                && column.getType() instanceof DataType.CollectionType
                && column.getType().isFrozen();
        }

        /**
         * Return whether this index is a 'ENTRIES' index on a map, e.g.,
         * CREATE INDEX ON mytable (ENTRIES(mymap))
         * 
         * @return {@code true} if this is an 'ENTRIES' index on a map.
         */
        public boolean isEntries() {
            return getOption(INDEX_MAP_ENTRIES) != null;
        }

        /**
         * Return the value for the given option name.
         * 
         * @param name Option name
         * @return Option value
         */
        public String getOption(String name) {
            return indexOptions != null ? indexOptions.get(name) : null;
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
            TableMetadata table = column.getTable();
            String ksName = Metadata.escapeId(table.getKeyspace().getName());
            String cfName = Metadata.escapeId(table.getName());
            String colName = Metadata.escapeId(column.getName());
            return isCustomIndex()
                ? String.format("CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s' WITH OPTIONS = %s;",
                    name, ksName, cfName, colName, getIndexClassName(), getOptionsAsCql())
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
            Iterator<Entry<String, String>> it = indexOptions.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, String> option = it.next();
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

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            IndexMetadata that = (IndexMetadata)o;

            return this.name.equals(that.name)
                && Objects.equal(this.indexOptions, that.indexOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name, indexOptions);
        }

        private static IndexMetadata build(ColumnMetadata column, Map<String, String> indexColumns) {
            if (indexColumns.isEmpty())
                return null;

            String type = indexColumns.get(INDEX_TYPE);
            if (type == null)
                return null;

            // Special case check for the value of the index_options column being a string with value 'null' as this
            // column appears to be set this way (JAVA-834).
            String indexOptionsCol = indexColumns.get(INDEX_OPTIONS);
            if (indexOptionsCol == null || indexOptionsCol.isEmpty() || indexOptionsCol.equals("null"))
                return new IndexMetadata(column, indexColumns.get(INDEX_NAME));
            else {
                Map<String, String> indexOptions = SimpleJSONParser.parseStringMap(indexOptionsCol);
                return new IndexMetadata(column, indexColumns.get(INDEX_NAME), indexOptions);
            }

        }
    }

    @Override
    public String toString() {
        String str = Metadata.escapeId(name) + ' ' + type;
        return isStatic ? str + " static" : str;
    }

    // Temporary class that is used to make building the schema easier. Not meant to be
    // exposed publicly at all.
    static class Raw {

        public enum Kind { PARTITION_KEY, CLUSTERING_KEY, REGULAR, COMPACT_VALUE, STATIC }

        public final String name;
        public final Kind kind;
        public final int componentIndex;
        public final DataType dataType;
        public final boolean isReversed;

        public final Map<String, String> indexColumns = new HashMap<String, String>();

        Raw(String name, Kind kind, int componentIndex, DataType dataType, boolean isReversed) {
            this.name = name;
            this.kind = kind;
            this.componentIndex = componentIndex;
            this.dataType = dataType;
            this.isReversed = isReversed;
        }

        static Raw fromRow(Row row, VersionNumber version, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {

            String name = row.getString(COLUMN_NAME);
            Kind kind = version.getMajor() < 2 || row.isNull(KIND)
                ? Kind.REGULAR
                : Enum.valueOf(Kind.class, row.getString(KIND).toUpperCase());
            int componentIndex = row.isNull(COMPONENT_INDEX) ? 0 : row.getInt(COMPONENT_INDEX);
            String validatorStr = row.getString(VALIDATOR);
            boolean reversed = CassandraTypeParser.isReversed(validatorStr);
            DataType dataType = CassandraTypeParser.parseOne(validatorStr, protocolVersion, codecRegistry);
            Raw c = new Raw(name, kind, componentIndex, dataType, reversed);

            for (String str : Arrays.asList(INDEX_TYPE, INDEX_NAME, INDEX_OPTIONS))
                if (!row.isNull(str))
                    c.indexColumns.put(str, row.getString(str));

            return c;
        }
    }
}
