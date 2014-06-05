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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

/**
 * The concrete definition of a user defined type (UDT).
 * <p>
 * A UDT is a essentially a named collection of fields (with a name and a type).
 */
public class UDTDefinition implements Iterable<UDTDefinition.Field> {

    private static final String TYPE_NAME = "type_name";
    private static final String COLS_NAMES = "field_names";
    private static final String COLS_TYPES = "field_types";

    private final String keyspace;
    private final String name;

    // Note that we don't expose the order of fields, from an API perspective this is a map
    // of String->Field, but internally we care about the order because the serialization format
    // of UDT expects a particular order.
    final Field[] byIdx;
    // For a given name, we can only have one field with that name, so we don't need a int[] in
    // practice. However, storing one element arrays save allocations in UDTValue.getAllIndexesOf
    // implementation.
    final Map<String, int[]> byName;

    UDTDefinition(String keyspace, String name, Collection<Field> fields) {
        this.keyspace = keyspace;
        this.name = name;
        this.byIdx = fields.toArray(new Field[fields.size()]);

        ImmutableMap.Builder<String, int[]> builder = new ImmutableMap.Builder<String, int[]>();
        for (int i = 0; i < byIdx.length; i++)
            builder.put(byIdx[i].getName(), new int[]{ i });
        this.byName = builder.build();
    }

    static UDTDefinition build(Row row) {
        String keyspace = row.getString(KeyspaceMetadata.KS_NAME);
        String name = row.getString(TYPE_NAME);

        List<String> fieldNames = row.getList(COLS_NAMES, String.class);
        List<String> fieldTypes = row.getList(COLS_TYPES, String.class);

        List<Field> fields = new ArrayList<Field>(fieldNames.size());
        for (int i = 0; i < fieldNames.size(); i++)
            fields.add(new Field(fieldNames.get(i), CassandraTypeParser.parseOne(fieldTypes.get(i))));

        return new UDTDefinition(keyspace, name, fields);
    }

    /**
     * Returns a new empty value for this user type definition.
     *
     * @return an empty value for this user type definition.
     */
    public UDTValue newValue() {
        return new UDTValue(this);
    }

    /**
     * The name of the keyspace this UDT is part of.
     *
     * @return the name of the keyspace this UDT is part of.
     */
    public String getKeyspace() {
        return keyspace;
    }

    /**
     * The name of this UDT.
     *
     * @return the name of this UDT.
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the number of fields in this UDT.
     *
     * @return the number of fields in this UDT.
     */
    public int size() {
        return byIdx.length;
    }

    /**
     * Returns whether this UDT contains a given field.
     *
     * @param name the name to check. Note that {@code name} obey the usual
     * CQL identifier rules: it should be quoted if it denotes a case sensitive
     * identifier (you can use {@link Metadata#quote} for the quoting).
     * @return {@code true} if this UDT contains a field named {@code name},
     * {@code false} otherwise.
     */
    public boolean contains(String name) {
        return byName.containsKey(Metadata.handleId(name));
    }

    /**
     * Returns an iterator over the fields of this UDT.
     *
     * @return an iterator over the fields of this UDT.
     */
    @Override
    public Iterator<Field> iterator() {
        return Iterators.forArray(byIdx);
    }

    /**
     * Returns the names of the fields of this UDT.
     *
     * @return the names of the fields of this UDT as a collection.
     */
    public Collection<String> getFieldNames() {
        return byName.keySet();
    }

    /**
     * Returns the type of a given field.
     *
     * @param name the name of the field. Note that {@code name} obey the usual
     * CQL identifier rules: it should be quoted if it denotes a case sensitive
     * identifier (you can use {@link Metadata#quote} for the quoting).
     * @return the type of field {@code name} if this UDT has a field of this
     * name, {@code null} otherwise.
     *
     * @throws IllegalArgumentException if {@code name} is not a field of this
     * UDT definition.
     */
    public DataType getFieldType(String name) {
        int[] idx = byName.get(Metadata.handleId(name));
        if (idx == null)
            throw new IllegalArgumentException(name + " is not a field defined in this definition");

        return byIdx[idx[0]].getType();
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(new Object[]{ keyspace, name, byIdx });
    }

    @Override
    public final boolean equals(Object o) {
        if(!(o instanceof UDTDefinition))
            return false;

        UDTDefinition other = (UDTDefinition)o;

        // Note: we don't test byName because it's redundant with byIdx in practice,
        // but also because the map holds 'int[]' which don't have proper equal.
        return keyspace.equals(other.keyspace)
            && name.equals(other.name)
            && Arrays.equals(byIdx, other.byIdx);
    }

    /**
     * Returns a CQL query representing this user type in human readable form.
     * <p>
     * This method is equivalent to {@link #asCQLQuery} but the ouptut is
     * formatted to be human readable (for some definition of human readable).
     *
     * @return the CQL query representing this user type.
     */
    public String exportAsString() {
        return asCQLQuery(true);
    }

    /**
     * Returns a CQL query representing this user type.
     * <p>
     * This method returns a single 'CREATE TYPE' query corresponding
     * to this UDT definition.
     * <p>
     * Note that the returned string is a single line; the returned query
     * is not formatted in any way.
     *
     * @return the 'CREATE TYPE' query corresponding to this user type.
     * @see #exportAsString
     */
    public String asCQLQuery() {
        return asCQLQuery(false);
    }

    private String asCQLQuery(boolean formatted) {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TYPE ").append(Metadata.escapeId(keyspace)).append('.').append(Metadata.escapeId(name)).append(" (");
        TableMetadata.newLine(sb, formatted);
        for (int i = 0; i < byIdx.length; i++) {
            sb.append(TableMetadata.spaces(4, formatted)).append(byIdx[i]);
            if (i < byIdx.length - 1)
                sb.append(',');
            TableMetadata.newLine(sb, formatted);
        }

        return sb.append(')').toString();
    }

    @Override
    public String toString() {
        return asCQLQuery();
    }

    /**
     * A UDT field.
     */
    public static class Field {
        private final String name;
        private final DataType type;

        Field(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

        /**
         * Returns the name of the field.
         *
         * @return the name of the field.
         */
        public String getName() {
            return name;
        }

        /**
         * Returns the type of the field.
         *
         * @return the type of the field.
         */
        public DataType getType() {
            return type;
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{ name, type });
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof Field))
                return false;

            Field other = (Field)o;
            return name.equals(other.name)
                && type.equals(other.type);
        }

        @Override
        public String toString() {
            return Metadata.escapeId(name) + ' ' + type;
        }
    }
}
