package com.datastax.driver.core;

import java.util.*;

import org.apache.cassandra.cql3.ColumnSpecification;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Metadata describing the columns returned in a {@link ResultSet} or a
 * {@link PreparedStatement}.
 * <p>
 * A {@code columnDefinitions}} instance is mainly a list of
 * {@code ColumnsDefinitions.Definition}. The definition/metadata for a column
 * can be accessed in one of two ways:
 * <ul>
 *   <li>by index</li>
 *   <li>by name</li>
 * </ul>
 * <p>
 * When accessed by name, column selection is case insentive. In case multiple
 * columns only differ by the case of their name, then the column returned with
 * be the one that has been defined in CQL without forcing case sensitivity
 * (i.e. it has either been defined without quotes, or it is fully lowercase).
 * If none of the columns have been thus defined, the first column matching
 * (with case insensitivity) is returned. You can however always force the case
 * of a selection by double quoting the name.
 * <p>
 * So for instance:
 * <ul>
 *   <li>If {@code cd} contains column {@code fOO}, then {@code cd.contains("foo")},
 *   {@code cd.contains("fOO")} and {@code cd.contains("Foo")} will return {@code true}.</li>
 *   <li>If {@code cd} contains both of {@code foo} and {@code FOO} then:
 *      <ul>
 *          <li>{@code cd.getType("foo")}, {@code cd.getType("fOO")} and {@code cd.getType("FOO")}
 *          will all match column {@code foo}.</li>
 *          <li>{@code cd.getType("\"FOO\"")} will match column {@code FOO}</li>
 *      </ul>
 * </ul>
 */
public class ColumnDefinitions implements Iterable<ColumnDefinitions.Definition> {

    static final ColumnDefinitions EMPTY = new ColumnDefinitions(new Definition[0]);

    private final Definition[] byIdx;
    private final Map<String, Integer> byName;

    ColumnDefinitions(Definition[] defs) {

        this.byIdx = defs;
        this.byName = new HashMap<String, Integer>(defs.length);

        for (int i = 0; i < defs.length; i++) {
            String name = defs[i].name;
            String lowerCased = name.toLowerCase();
            Integer previous = this.byName.put(lowerCased, i);
            if (previous != null) {
                // We have 2 columns that only differ by case. If one has been defined with
                // "case insensitivity", set this one, otherwise keep the first found.
                if (name.equals(lowerCased)) {
                    assert !defs[previous].name.equals(lowerCased);
                    this.byName.put(defs[previous].name, previous);
                } else {
                    this.byName.put(lowerCased, previous);
                    this.byName.put(name, i);
                }
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
     * @return {@code true} if this metadata contains the column named {@code  name},
     * {@code false} otherwise.
     */
    public boolean contains(String name) {
        return findIdx(name) != null;
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

    Integer findIdx(String name) {
        String trimmed = name.trim();
        if (trimmed.length() >= 2 && trimmed.charAt(0) == '"' && trimmed.charAt(trimmed.length() - 1) == '"')
            return byName.get(name.substring(1, trimmed.length() - 1));

        return byName.get(name.toLowerCase());
    }

    int getIdx(String name) {
        Integer idx = findIdx(name);
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
