package com.datastax.driver.core;

import java.util.*;

import com.datastax.driver.core.transport.Codec;

import org.apache.cassandra.cql3.ColumnSpecification;

/**
 * Metadata describing the columns returned in a {@link ResultSet} or a
 * {@link PreparedStatement}.
 */
public class Columns implements Iterable<Columns.Definition> {

    static final Columns EMPTY = new Columns(new Definition[0]);

    private final Definition[] byIdx;
    private final Map<String, Integer> byName;

    Columns(Definition[] defs) {

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
    public int count() {
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
     * Returns the list of the names for the columns defined in these metadata.
     *
     * @return the list of the names for the columns defined in these metadata.
     * The names in the returned list will be in the order of this metadata.
     */
    public List<String> names() {
        List<String> names = new ArrayList<String>(byIdx.length);
        for (Definition def : byIdx)
            names.add(def.name);
        return names;
    }

    /**
     * Returns the list of the types for the columns defined in these metadata.
     *
     * @return the list of the types for the columns defined in these metadata.
     * The types in the returned list will be in the order of this metadata.
     */
    public List<DataType> types() {
        List<DataType> types = new ArrayList<DataType>(byIdx.length);
        for (Definition def : byIdx)
            types.add(def.type);
        return types;
    }

    /**
     * Returns the name of the {@code i}th column in this metadata.
     *
     * @return the name of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= count()}
     */
    public String name(int i) {
        return byIdx[i].name;
    }

    /**
     * Returns the type of the {@code i}th column in this metadata.
     *
     * @return the type of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= count()}
     */
    public DataType type(int i) {
        return byIdx[i].type;
    }

    /**
     * Returns the type of column {@code name} in this metadata.
     *
     * @return the type of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public DataType type(String name) {
        return type(getIdx(name));
    }

    /**
     * Returns the keyspace of the {@code i}th column in this metadata.
     *
     * @return the keyspace of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= count()}
     */
    public String keyspace(int i) {
        return byIdx[i].keyspace;
    }

    /**
     * Returns the keyspace of column {@code name} in this metadata.
     *
     * @return the keyspace of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public String keyspace(String name) {
        return keyspace(getIdx(name));
    }

    /**
     * Returns the table of the {@code i}th column in this metadata.
     *
     * @return the table of the {@code i}th column in this metadata.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0} or {@code i >= count()}
     */
    public String table(int i) {
        return byIdx[i].table;
    }

    /**
     * Returns the table of column {@code name} in this metadata.
     *
     * @return the table of column {@code name} in this metadata.
     *
     * @throws IllegalArgumentException if {@code name} is not one of the columns in this metadata.
     */
    public String table(String name) {
        return table(getIdx(name));
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Columns[");
        for (int i = 0; i < count(); i++) {
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
        if (i < 0 || i >= count())
            throw new ArrayIndexOutOfBoundsException(i);
    }

    DataType.Native checkType(int i, DataType.Native... types) {
        DataType defined = type(i);
        for (DataType.Native type : types)
            if (type == defined)
                return type;

        throw new InvalidTypeException(String.format("Column %s is of type %s", name(i), defined));
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
