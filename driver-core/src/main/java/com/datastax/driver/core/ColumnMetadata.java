package com.datastax.driver.core;

import java.util.*;

import com.datastax.driver.core.transport.Codec;

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

    ColumnMetadata(TableMetadata table, String name, DataType type, CQLRow row) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.index = IndexMetadata.build(this, row);
    }

    static ColumnMetadata build(TableMetadata tm, CQLRow row) {
        try {
            String name = row.getString(COLUMN_NAME);
            AbstractType<?> t = TypeParser.parse(row.getString(VALIDATOR));
            ColumnMetadata cm = new ColumnMetadata(tm, name, Codec.rawTypeToDataType(t), row);
            tm.add(cm);
            return cm;
        } catch (RequestValidationException e) {
            // The server will have validated the type
            throw new RuntimeException(e);
        }
    }

    /**
     * The name of the column.
     *
     * @return the name of the column.
     */
    public String getName() {
        return name;
    }

    /**
     * The metadata of the table this column is part of.
     *
     * @return the {@code TableMetadata} for the table this column is part of.
     */
    public TableMetadata getTable() {
        return table;
    }

    /**
     * The type of the column.
     *
     * @return the type of the column.
     */
    public DataType getType() {
        return type;
    }

    /**
     * The indexing metadata on this column if the column is indexed.
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
        // It doesn't make sense to expose the index type for CQL3 at this
        // point (the notion don't exist yet in CQL), but keeping it internally
        // so we don't forget it exists
        private final String type;
        private final Map<String, String> options = new HashMap<String, String>();

        private IndexMetadata(ColumnMetadata column, String name, String type) {
            this.column = column;
            this.name = name;
            this.type = type;
        }

        /**
         * The column this index metadata refers to.
         *
         * @return the column this index metadata refers to.
         */
        public ColumnMetadata getIndexedColumn() {
            return column;
        }

        /**
         * The index name.
         *
         * @return the index name.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns a CQL query representing this index.
         *
         * This method returns a single 'CREATE INDEX' query with the options
         * corresponding to this index definition.
         *
         * @return the 'CREATE INDEX' query corresponding to this index.
         */
        public String asCQLQuery() {
            TableMetadata table = column.getTable();
            return String.format("CREATE INDEX %s ON %s.%s (%s)", name, table.getKeyspace().getName(), table.getName(), column.getName());
        }

        private static IndexMetadata build(ColumnMetadata column, CQLRow row) {
            String type = row.getString(INDEX_TYPE);
            if (type == null)
                return null;

            IndexMetadata index = new IndexMetadata(column, type, row.getString(INDEX_NAME));
            // TODO: handle options
            return index;
        }
    }

    @Override
    public String toString() {
        return name + " " + type;
    }
}
