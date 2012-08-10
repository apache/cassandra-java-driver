package com.datastax.driver.core;

import java.util.*;

import com.datastax.driver.core.transport.Codec;

import org.apache.cassandra.config.ConfigurationException;
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
    private final Index index;

    ColumnMetadata(TableMetadata table, String name, DataType type, Index index) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.index = index;
    }

    static ColumnMetadata build(TableMetadata tm, CQLRow row) {
        try {
            String name = row.getString(COLUMN_NAME);
            AbstractType<?> t = TypeParser.parse(row.getString(VALIDATOR));
            ColumnMetadata cm = new ColumnMetadata(tm, name, Codec.rawTypeToDataType(t), Index.build(row));
            tm.add(cm);
            return cm;
        } catch (ConfigurationException e) {
            // The server will have validated the type
            throw new RuntimeException(e);
        }
    }

    public String getName() {
        return name;
    }

    static class Index {

        private static final String INDEX_TYPE = "index_type";
        private static final String INDEX_OPTIONS = "index_options";
        private static final String INDEX_NAME = "index_name";

        public final String name;
        public final String type;
        public final Map<String, String> options = new HashMap<String, String>();

        private Index(String name, String type) {
            this.name = name;
            this.type = type;
        }

        public static Index build(CQLRow row) {
            String type = row.getString(INDEX_TYPE);
            if (type == null)
                return null;

            Index index = new Index(type, row.getString(INDEX_NAME));
            // TODO: handle options
            return index;
        }
    }

    @Override
    public String toString() {
        return name + " " + type;
    }
}
