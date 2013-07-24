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

import org.apache.cassandra.cql3.ColumnSpecification;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Metadata describing the columns returned in a {@link ResultSet} or a
 * {@link PreparedStatement}.
 * <p>
 * A {@code columnDefinitions}} instance is mainly a list of
 * {@code ColumnsDefinitions.Definition}. The definitions or metadata for a column
 * can be accessed either by:
 * <ul>
 *   <li>index (indexed from 0)</li>
 *   <li>name</li>
 * </ul>
 * <p>
 * When accessed by name, column selection is case insensitive. In case multiple
 * columns only differ by the case of their name, then the column returned with
 * be the first column that has been defined in CQL without forcing case sensitivity
 * (that is, it has either been defined without quotes or is fully lowercase).
 * If none of the columns have been defined in this manner, the first column matching
 * (with case insensitivity) is returned. You can force the case of a selection
 * by double quoting the name.
 * <p>
 * For example:
 * <ul>
 *   <li>If {@code cd} contains column {@code fOO}, then {@code cd.contains("foo")},
 *   {@code cd.contains("fOO")} and {@code cd.contains("Foo")} will return {@code true}.</li>
 *   <li>If {@code cd} contains both {@code foo} and {@code FOO} then:
 *      <ul>
 *          <li>{@code cd.getType("foo")}, {@code cd.getType("fOO")} and {@code cd.getType("FOO")}
 *          will all match column {@code foo}.</li>
 *          <li>{@code cd.getType("\"FOO\"")} will match column {@code FOO}</li>
 *      </ul>
 * </ul>
 * Note that the preceding rules mean that if a {@code ColumnDefinitions} object
 * contains multiple occurrences of the exact same name (be it the same column
 * multiple times or columns from different tables with the same name), you
 * will have to use selection by index to disambiguate.
 */
public class ColumnDefinitions implements Iterable<ColumnDefinitions.Definition> {

    static final ColumnDefinitions EMPTY = new ColumnDefinitions(new Definition[0]);

    private final Definition[] byIdx;
    private final Map<String, int[]> byName;

    ColumnDefinitions(Definition[] defs) {

        this.byIdx = defs;
        this.byName = new HashMap<String, int[]>(defs.length);

        for (int i = 0; i < defs.length; i++) {
            // Be optimistic, 99% of the time, previous will be null.
            int[] previous = this.byName.put(defs[i].name.toLowerCase(), new int[]{ i });
            if (previous != null) {
                int[] indexes = new int[previous.length + 1];
                System.arraycopy(previous, 0, indexes, 0, previous.length);
                indexes[indexes.length - 1] = i;
                this.byName.put(defs[i].name.toLowerCase(), indexes);
            }
        }
    }

    /**
     * Returns the number of columns described by this {@code Columns}
     * instance.
     *
     * @return the number of columns described by this metadata.
     */
    public int size() {
        return byIdx.length;
    }

    /**
     * Returns whether this metadata contains a given column.
     *
     * @param name the name of column.
     * @return {@code true} if this metadata contains the column named {@code  name},
     * {@code false} otherwise.
     */
    public boolean contains(String name) {
        return findIdx(name) >= 0;
    }

    /**
     * The index in this metadata of the povided column name, if present.
     *
     * @param name the name of the column.
     * @return the index of {@code name} in this metadata if this metadata
     * {@code contains(name)}, -1 otherwise.
     */
    public int getIndexOf(String name) {
        return findIdx(name);
    }

    /**
     * Returns an iterator over the {@link Definition} contained in this metadata.
     *
     * The order of the iterator will be the one of this metadata.
     *
     * @return an iterator over the {@link Definition} contained in this metadata.
     */
    @Override
    public Iterator<Definition> iterator() {
        return Arrays.asList(byIdx).iterator();
    }

    /**
     * Returns a list containing all the definitions of this metadata in order.
     *
     * @return a list of the {@link Definition} contained in this metadata.
     */
    public List<Definition> asList() {
        return Arrays.asList(byIdx);
    }

    /**
     * Returns the name of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the name of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getName(int i) {
        return byIdx[i].name;
    }

    /**
     * Returns the type of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the type of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public DataType getType(int i) {
        return byIdx[i].type;
    }

    /**
     * Returns the type of column {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the type of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public DataType getType(String name) {
        return getType(getIdx(name));
    }

    /**
     * Returns the keyspace of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the keyspace of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getKeyspace(int i) {
        return byIdx[i].keyspace;
    }

    /**
     * Returns the keyspace of column {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the keyspace of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public String getKeyspace(String name) {
        return getKeyspace(getIdx(name));
    }

    /**
     * Returns the table of the {@code i}th column in this metadata.
     *
     * @param i the index in this metadata.
     * @return the table of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= size()}
     */
    public String getTable(int i) {
        return byIdx[i].table;
    }

    /**
     * Returns the table of column {@code name} in this metadata.
     *
     * @param name the name of the column.
     * @return the table of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public String getTable(String name) {
        return getTable(getIdx(name));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Columns[");
        for (int i = 0; i < size(); i++) {
            if (i != 0)
                sb.append(", ");
            Definition def = byIdx[i];
            sb.append(def.name).append("(").append(def.type).append(")");
        }
        sb.append("]");
        return sb.toString();
    }

    int findIdx(String name) {
        boolean caseSensitive = false;
        if (name.length() >= 2 && name.charAt(0) == '"' && name.charAt(name.length() - 1) == '"') {
            name = name.substring(1, name.length() - 1);
            caseSensitive = true;
        }

        int[] indexes = byName.get(name.toLowerCase());
        if (indexes == null) {
            return -1;
        } else if (indexes.length == 1) {
            return indexes[0];
        } else {
            for (int i = 0; i < indexes.length; i++) {
                int idx = indexes[i];
                if (caseSensitive) {
                    if (name.equals(byIdx[idx].name))
                        return idx;
                } else {
                    if (name.toLowerCase().equals(byIdx[idx].name))
                        return idx;
                }
            }
            if (caseSensitive)
                return -1;
            else
                return indexes[0];
        }
    }

    int getIdx(String name) {
        int idx = findIdx(name);
        if (idx < 0)
            throw new IllegalArgumentException(name + " is not a column defined in this metadata");

        return idx;
    }

    void checkBounds(int i) {
        if (i < 0 || i >= size())
            throw new ArrayIndexOutOfBoundsException(i);
    }

    // Note: we avoid having a vararg method to avoid the array allocation that comes with it.
    void checkType(int i, DataType.Name name) {
        DataType defined = getType(i);
        if (name != defined.getName())
            throw new InvalidTypeException(String.format("Column %s is of type %s", getName(i), defined));
    }

    DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName())
            throw new InvalidTypeException(String.format("Column %s is of type %s", getName(i), defined));

        return defined.getName();
    }

    DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2, DataType.Name name3) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName() && name3 != defined.getName())
            throw new InvalidTypeException(String.format("Column %s is of type %s", getName(i), defined));

        return defined.getName();
    }

    /**
     * A column definition.
     */
    public static class Definition {

        private final String keyspace;
        private final String table;
        private final String name;
        private final DataType type;

        Definition(String keyspace, String table, String name, DataType type) {
            this.keyspace = keyspace;
            this.table = table;
            this.name = name;
            this.type = type;
        }

        static Definition fromTransportSpecification(ColumnSpecification spec) {
            return new Definition(spec.ksName, spec.cfName, spec.name.toString(), Codec.rawTypeToDataType(spec.type));
        }

        /**
         * The name of the keyspace this column is part of.
         *
         * @return the name of the keyspace this column is part of.
         */
        public String getKeyspace() {
            return keyspace;
        }

        /**
         * Returns the name of the table this column is part of.
         *
         * @return the name of the table this column is part of.
         */
        public String getTable() {
            return table;
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
         * Returns the type of the column.
         *
         * @return the type of the column.
         */
        public DataType getType() {
            return type;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{ keyspace, table, name, type});
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof Definition))
                return false;

            Definition other = (Definition)o;
            return keyspace.equals(other.keyspace)
                && table.equals(other.table)
                && name.equals(other.name)
                && type.equals(other.type);
        }
    }
}
