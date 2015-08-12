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
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

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

    private final TableMetadata table;
    private final String name;
    private final DataType type;
    private final boolean isStatic;

    // this is the "reverse" side of the many-to-many relationship
    // between columns and indexes, and is updated only after the column is created
    final Map<String, IndexMetadata> indexes = new LinkedHashMap<String, IndexMetadata>();

    private ColumnMetadata(TableMetadata table, String name, DataType type, boolean isStatic) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.isStatic = isStatic;
    }

    static ColumnMetadata fromRaw(TableMetadata tm, Raw raw) {
        return new ColumnMetadata(tm, raw.name, raw.dataType, raw.kind == Raw.Kind.STATIC);
    }

    static ColumnMetadata forAlias(TableMetadata tm, String name, DataType type) {
        return new ColumnMetadata(tm, name, type, false);
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
     * Whether this column is a static column.
     *
     * @return Whether this column is a static column or not.
     */
    public boolean isStatic() {
        return isStatic;
    }

    /**
     * Returns metadata on a index on this column.
     *
     * @param name the name of the index to retrieve ({@code name} will be
     * interpreted as a case-insensitive identifier unless enclosed in double-quotes,
     * see {@link Metadata#quote}).
     * @return the metadata for the {@code name} index if it exists, or
     * {@code null} otherwise.
     */
    public IndexMetadata getIndex(String name) {
        return indexes.get(Metadata.handleId(name));
    }

    /**
     * Returns a list containing all the indexes on this column.
     *
     * @return a list containing the metadata for the indexes on this column.
     */
    public List<IndexMetadata> getIndexes() {
        return new ArrayList<IndexMetadata>(indexes.values());
    }

    @Override
    public String toString() {
        String str = Metadata.escapeId(name) + ' ' + type;
        return isStatic ? str + " static" : str;
    }

    // Temporary class that is used to make building the schema easier. Not meant to be
    // exposed publicly at all.
    static class Raw {

        public enum Kind {

            PARTITION_KEY     ("PARTITION_KEY" , "PARTITION_KEY"),
            CLUSTERING_COLUMN ("CLUSTERING_KEY", "CLUSTERING"   ),
            REGULAR           ("REGULAR"       , "REGULAR"      ),
            COMPACT_VALUE     ("COMPACT_VALUE" , ""             ), // v2 only
            STATIC            ("STATIC"        , "STATIC"       );

            final String v2;
            final String v3;

            Kind(String v2, String v3) {
                this.v2 = v2;
                this.v3 = v3;
            }

            static Kind fromStringV2(String s) {
                for (Kind kind : Kind.values()) {
                    if(kind.v2.equalsIgnoreCase(s))
                        return kind;
                }
                throw new IllegalArgumentException(s);
            }

            static Kind fromStringV3(String s) {
                for (Kind kind : Kind.values()) {
                    if(kind.v3.equalsIgnoreCase(s))
                        return kind;
                }
                throw new IllegalArgumentException(s);
            }
        }

        public final String name;
        public Kind kind;
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
            Kind kind;
            if(version.getMajor() < 2 || row.isNull(KIND)) {
                kind = Kind.REGULAR;
            } else if (version.getMajor() < 3) {
                kind = Kind.fromStringV2(row.getString(KIND));
            } else {
                kind = Kind.fromStringV3(row.getString(KIND));
            }
            int componentIndex = row.isNull(COMPONENT_INDEX) ? 0 : row.getInt(COMPONENT_INDEX);
            String validatorStr = row.getString(VALIDATOR);
            boolean reversed = CassandraTypeParser.isReversed(validatorStr);
            DataType dataType = CassandraTypeParser.parseOne(validatorStr, protocolVersion, codecRegistry);
            Raw c = new Raw(name, kind, componentIndex, dataType, reversed);

            // secondary indexes (C* < 3.0.0)
            // from C* 3.0 onwards 2i are defined in a separate table
            for (String str : Arrays.asList(INDEX_TYPE, INDEX_NAME, INDEX_OPTIONS))
                if (row.getColumnDefinitions().contains(str) && !row.isNull(str))
                    c.indexColumns.put(str, row.getString(str));

            return c;
        }
    }
}
