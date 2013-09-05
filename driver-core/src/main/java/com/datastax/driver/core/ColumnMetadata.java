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
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;

/**
 * Describes a Column.
 */
public class ColumnMetadata {

    private static final String COLUMN_NAME = "column_name";
    private static final String VALIDATOR = "validator";
    private static final String INDEX = "component_index";

    private final TableMetadata table;
    private final String name;
    private final DataType type;
    private final IndexMetadata index;

    ColumnMetadata(TableMetadata table, String name, DataType type, Row row) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.index = IndexMetadata.build(this, row);
    }

    static ColumnMetadata build(TableMetadata tm, Row row) {
        try {
            String name = row.getString(COLUMN_NAME);

            String validator = row.getString(VALIDATOR);
            AbstractType<?> t = TypeParser.parse(TableMetadata.fixTimestampType(validator));
            ColumnMetadata cm = new ColumnMetadata(tm, name, Codec.rawTypeToDataType(t), row);
            tm.add(cm);
            return cm;
        } catch (RequestValidationException e) {
            // The server will have validated the type
            throw new RuntimeException(e);
        }
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

        private static final String INDEX_TYPE = "index_type";
        private static final String INDEX_OPTIONS = "index_options";
        private static final String INDEX_NAME = "index_name";

        private final ColumnMetadata column;
        private final String name;
        // It doesn't make sense to expose the index type, not the index
        // options for CQL3 at this point since the notion doesn't exist yet in CQL3.
        // But keeping the variable internally so we don't forget it exists.
        private final String type;
        private final Map<String, String> options = new HashMap<String, String>();

        private IndexMetadata(ColumnMetadata column, String name, String type) {
            this.column = column;
            this.name = name;
            this.type = type;
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
         * Returns a CQL query representing this index.
         *
         * This method returns a single 'CREATE INDEX' query corresponding to
         * this index definition.
         *
         * @return the 'CREATE INDEX' query corresponding to this index.
         */
        public String asCQLQuery() {
            TableMetadata table = column.getTable();
            return String.format("CREATE INDEX %s ON %s.%s (%s)", name, table.getKeyspace().getName(), table.getName(), column.getName());
        }

        private static IndexMetadata build(ColumnMetadata column, Row row) {
            if (row == null)
                return null;

            String type = row.getString(INDEX_TYPE);
            if (type == null)
                return null;

            return new IndexMetadata(column, row.getString(INDEX_NAME), type);
        
        }
    }

    @Override
    public String toString() {
        return name + " " + type;
    }
}
