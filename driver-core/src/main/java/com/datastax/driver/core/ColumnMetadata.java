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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes a Column.
 */
public class ColumnMetadata {

    private static final String COLUMN_NAME = "column_name";
    private static final String VALIDATOR = "validator";
    private static final String COMPONENT_INDEX = "component_index";
    private static final String KIND = "type";

    private static final String INDEX_TYPE = "index_type";
    private static final String INDEX_OPTIONS = "index_options";
    private static final String INDEX_NAME = "index_name";
    private static final String CUSTOM_INDEX_CLASS = "class_name";

    private final TableMetadata table;
    private final String name;
    private final DataType type;
    private final IndexMetadata index;

    private ColumnMetadata(TableMetadata table, String name, DataType type, Map<String, String> indexColumns) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.index = IndexMetadata.build(this, indexColumns);
    }

    static ColumnMetadata fromRaw(TableMetadata tm, Raw raw) {
        return new ColumnMetadata(tm, raw.name, raw.dataType, raw.indexColumns);
    }

    static ColumnMetadata forAlias(TableMetadata tm, String name, DataType type) {
        return new ColumnMetadata(tm, name, type, Collections.<String, String>emptyMap());
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
     * Metadata on a column index.
     */
    public static class IndexMetadata {

        private final ColumnMetadata column;
        private final String name;
        private final String customClassName; // will be null, unless it's a custom index

        private IndexMetadata(ColumnMetadata column, String name, String customClassName) {
            this.column = column;
            this.name = name;
            this.customClassName = customClassName;
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
            return customClassName != null;
        }

        /**
         * The name of the class used to implement the custom index, if it is one.
         *
         * @return the name of the class used Cassandra side to implement this
         * custom index if {@code isCustomIndex() == true}, {@code null} otherwise.
         */
        public String getIndexClassName() {
            return customClassName;
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
                 ? String.format("CREATE CUSTOM INDEX %s ON %s.%s (%s) USING '%s';", name, ksName, cfName, colName, customClassName)
                 : String.format("CREATE INDEX %s ON %s.%s (%s);", name, ksName, cfName, colName);
        }

        private static IndexMetadata build(ColumnMetadata column, Map<String, String> indexColumns) {
            if (indexColumns.isEmpty())
                return null;

            String type = indexColumns.get(INDEX_TYPE);
            if (type == null)
                return null;

            if (!type.equalsIgnoreCase("CUSTOM") || !indexColumns.containsKey(INDEX_OPTIONS))
                return new IndexMetadata(column, indexColumns.get(INDEX_NAME), null);

            Map<String, String> indexOptions = SimpleJSONParser.parseStringMap(indexColumns.get(INDEX_OPTIONS));
            return new IndexMetadata(column, indexColumns.get(INDEX_NAME), indexOptions.get(CUSTOM_INDEX_CLASS));
        }
    }

    @Override
    public String toString() {
        return Metadata.escapeId(name) + ' ' + type;
    }

    // Temporary class that is used to make building the schema easier. Not meant to be
    // exposed publicly at all.
    static class Raw {
        public enum Kind { PARTITION_KEY, CLUSTERING_KEY, REGULAR, COMPACT_VALUE }

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

        static Raw fromRow(Row row, VersionNumber version) {

            String name = row.getString(COLUMN_NAME);
            Kind kind = version.getMajor() < 2 || row.isNull(KIND)
                      ? Kind.REGULAR
                      : Enum.valueOf(Kind.class, row.getString(KIND).toUpperCase());
            int componentIndex = row.isNull(COMPONENT_INDEX) ? 0 : row.getInt(COMPONENT_INDEX);
            String validatorStr = row.getString(VALIDATOR);
            boolean reversed = CassandraTypeParser.isReversed(validatorStr);
            DataType dataType = CassandraTypeParser.parseOne(validatorStr);

            Raw c = new Raw(name, kind, componentIndex, dataType, reversed);

            for (String str : Arrays.asList(INDEX_TYPE, INDEX_NAME, INDEX_OPTIONS))
                if (!row.isNull(str))
                    c.indexColumns.put(str, row.getString(str));

            return c;
        }
    }
}
