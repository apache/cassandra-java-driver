package com.datastax.driver.core;

import java.util.*;

import org.apache.cassandra.cql3.ColumnSpecification;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Metadata describing the columns returned in a {@link ResultSet} or a
 * {@link PreparedStatement}.
 */
public class ColumnDefinitions implements Iterable<ColumnDefinitions.Definition> {

    static final ColumnDefinitions EMPTY = new ColumnDefinitions(new Definition[0]);

    private final Definition[] byIdx;
    private final Map<String, Integer> byName;

    ColumnDefinitions(Definition[] defs) {

        this.byIdx = defs;
        this.byName = new HashMap<String, Integer>(defs.length);

        for (int i = 0; i < defs.length; i++)
            this.byName.put(defs[i].name, i);
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
     * @return {@code true} if this metadata contains the column named {@code  name},
     * {@code false} otherwise.
     */
    public boolean contains(String name) {
        return byName.containsKey(name);
    }

    /**
     * Returns an iterator over the {@link Definition} contained in this metadata.
     *
     * The order of the iterator will be the one of this metadata.
     *
     * @return an iterator over the {@link Definition} contained in this metadata.
     */
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
     * @return the table of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public String getTable(String name) {
        return getTable(getIdx(name));
    }

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

    int getIdx(String name) {
        Integer idx = byName.get(name);
        if (idx == null)
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

        private Definition(String keyspace, String table, String name, DataType type) {

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
         * The name of the table this column is part of.
         *
         * @return the name of the table this column is part of.
         */
        public String getTable() {
            return table;
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
         * The type of the column.
         *
         * @return the type of the column.
         */
        public DataType getType() {
            return type;
        }
    }
}
