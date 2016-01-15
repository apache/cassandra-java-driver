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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes a Column.
 */
public class ColumnMetadata {

    static final String COLUMN_NAME = "column_name";

    static final String VALIDATOR = "validator"; // v2 only
    static final String TYPE = "type"; // replaces validator, v3 onwards

    static final String COMPONENT_INDEX = "component_index"; // v2 only
    static final String POSITION = "position"; // replaces component_index, v3 onwards

    static final String KIND_V2 = "type"; // v2 only
    static final String KIND_V3 = "kind"; // replaces type, v3 onwards

    static final String CLUSTERING_ORDER = "clustering_order";
    static final String DESC = "desc";

    static final String INDEX_TYPE = "index_type";
    static final String INDEX_OPTIONS = "index_options";
    static final String INDEX_NAME = "index_name";

    private final AbstractTableMetadata parent;
    private final String name;
    private final DataType type;
    private final boolean isStatic;

    private ColumnMetadata(AbstractTableMetadata parent, String name, DataType type, boolean isStatic) {
        this.parent = parent;
        this.name = name;
        this.type = type;
        this.isStatic = isStatic;
    }

    static ColumnMetadata fromRaw(AbstractTableMetadata tm, Raw raw, DataType dataType) {
        return new ColumnMetadata(tm, raw.name, dataType, raw.kind == Raw.Kind.STATIC);
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
     * Returns the parent object of this column. This can be a {@link TableMetadata}
     * or a {@link MaterializedViewMetadata} object.
     *
     * @return the parent object of this column.
     */
    public AbstractTableMetadata getParent() {
        return parent;
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

    @Override
    public boolean equals(Object other) {
        if (other == this)
            return true;
        if (!(other instanceof ColumnMetadata))
            return false;

        ColumnMetadata that = (ColumnMetadata) other;
        return this.name.equals(that.name) &&
                this.isStatic == that.isStatic &&
                this.type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, isStatic, type);
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

            PARTITION_KEY("PARTITION_KEY", "PARTITION_KEY"),
            CLUSTERING_COLUMN("CLUSTERING_KEY", "CLUSTERING"),
            REGULAR("REGULAR", "REGULAR"),
            COMPACT_VALUE("COMPACT_VALUE", ""), // v2 only
            STATIC("STATIC", "STATIC");

            final String v2;
            final String v3;

            Kind(String v2, String v3) {
                this.v2 = v2;
                this.v3 = v3;
            }

            static Kind fromStringV2(String s) {
                for (Kind kind : Kind.values()) {
                    if (kind.v2.equalsIgnoreCase(s))
                        return kind;
                }
                throw new IllegalArgumentException(s);
            }

            static Kind fromStringV3(String s) {
                for (Kind kind : Kind.values()) {
                    if (kind.v3.equalsIgnoreCase(s))
                        return kind;
                }
                throw new IllegalArgumentException(s);
            }
        }

        public final String name;
        public Kind kind;
        public final int position;
        public final String dataType;
        public final boolean isReversed;

        public final Map<String, String> indexColumns = new HashMap<String, String>();

        Raw(String name, Kind kind, int position, String dataType, boolean isReversed) {
            this.name = name;
            this.kind = kind;
            this.position = position;
            this.dataType = dataType;
            this.isReversed = isReversed;
        }

        static Raw fromRow(Row row, VersionNumber version) {
            String name = row.getString(COLUMN_NAME);

            Kind kind;
            if (version.getMajor() < 2) {
                kind = Kind.REGULAR;
            } else if (version.getMajor() < 3) {
                kind = row.isNull(KIND_V2) ? Kind.REGULAR : Kind.fromStringV2(row.getString(KIND_V2));
            } else {
                kind = row.isNull(KIND_V3) ? Kind.REGULAR : Kind.fromStringV3(row.getString(KIND_V3));
            }

            int position;
            if (version.getMajor() >= 3) {
                position = row.getInt(POSITION); // cannot be null, -1 is used as a special value instead of null to avoid tombstones
                if (position == -1) position = 0;
            } else {
                position = row.isNull(COMPONENT_INDEX) ? 0 : row.getInt(COMPONENT_INDEX);
            }

            String dataType;
            boolean reversed;
            if (version.getMajor() >= 3) {
                dataType = row.getString(TYPE);
                String clusteringOrderStr = row.getString(CLUSTERING_ORDER);
                reversed = clusteringOrderStr.equals(DESC);
            } else {
                dataType = row.getString(VALIDATOR);
                reversed = DataTypeClassNameParser.isReversed(dataType);
            }

            Raw c = new Raw(name, kind, position, dataType, reversed);

            // secondary indexes (C* < 3.0.0)
            // from C* 3.0 onwards 2i are defined in a separate table
            if (version.getMajor() < 3) {
                for (String str : Arrays.asList(INDEX_TYPE, INDEX_NAME, INDEX_OPTIONS))
                    if (row.getColumnDefinitions().contains(str) && !row.isNull(str))
                        c.indexColumns.put(str, row.getString(str));
            }
            return c;
        }
    }
}
