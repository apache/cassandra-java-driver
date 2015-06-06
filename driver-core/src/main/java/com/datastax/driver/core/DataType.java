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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Data types supported by cassandra.
 */
public class DataType {

    /**
     * The CQL type name.
     */
    public enum Name {

        ASCII     (1),
        BIGINT    (2),
        BLOB      (3),
        BOOLEAN   (4),
        COUNTER   (5),
        DECIMAL   (6),
        DOUBLE    (7),
        FLOAT     (8),
        INET      (16),
        INT       (9),
        TEXT      (10),
        TIMESTAMP (11),
        UUID      (12),
        VARCHAR   (13),
        VARINT    (14),
        TIMEUUID  (15),
        LIST      (32),
        SET       (34),
        MAP       (33),
        CUSTOM    (0);

        final int protocolId;

        private static final Name[] nameToIds;
        static {
            int maxCode = -1;
            for (Name name : Name.values())
                maxCode = Math.max(maxCode, name.protocolId);
            nameToIds = new Name[maxCode + 1];
            for (Name name : Name.values()) {
                if (nameToIds[name.protocolId] != null)
                    throw new IllegalStateException("Duplicate Id");
                nameToIds[name.protocolId] = name;
            }
        }

        private Name(int protocolId) {
            this.protocolId = protocolId;
        }

        static Name fromProtocolId(int id) {
            Name name = nameToIds[id];
            if (name == null)
                throw new DriverInternalError("Unknown data type protocol id: " + id);
            return name;
        }

        /**
         * Returns whether this data type name represent the name of a collection type
         * that is a list, set or map.
         *
         * @return whether this data type name represent the name of a collection type.
         */
        public boolean isCollection() {
            switch (this) {
                case LIST:
                case SET:
                case MAP:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    private final DataType.Name name;
    private final List<DataType> typeArguments;
    private final String customClassName;

    private static final Map<Name, DataType> primitiveTypeMap = new EnumMap<Name, DataType>(Name.class);
    static {
        for (Name name : Name.values()) {
            if (!name.isCollection() && name != Name.CUSTOM)
                primitiveTypeMap.put(name, new DataType(name, Collections.<DataType>emptyList()));
        }
    }
    private static final Set<DataType> primitiveTypeSet = ImmutableSet.copyOf(primitiveTypeMap.values());

    private DataType(DataType.Name name, List<DataType> typeArguments) {
        this(name, typeArguments, null);
    }

    private DataType(Name name, List<DataType> typeArguments, String customClassName) {
        this.name = name;
        this.typeArguments = typeArguments;
        this.customClassName = customClassName;
    }

    static DataType decode(ByteBuf buffer) {
        Name name = Name.fromProtocolId(buffer.readUnsignedShort());
        switch (name) {
            case CUSTOM:
                return custom(CBUtil.readString(buffer));
            case LIST:
                return list(decode(buffer));
            case SET:
                return set(decode(buffer));
            case MAP:
                DataType keys = decode(buffer);
                DataType values = decode(buffer);
                return map(keys, values);
            default:
                return primitiveTypeMap.get(name);
        }
    }

    /**
     * Returns the ASCII type.
     *
     * @return The ASCII type.
     */
    public static DataType ascii() {
        return primitiveTypeMap.get(Name.ASCII);
    }

    /**
     * Returns the BIGINT type.
     *
     * @return The BIGINT type.
     */
    public static DataType bigint() {
        return primitiveTypeMap.get(Name.BIGINT);
    }

    /**
     * Returns the BLOB type.
     *
     * @return The BLOB type.
     */
    public static DataType blob() {
        return primitiveTypeMap.get(Name.BLOB);
    }

    /**
     * Returns the BOOLEAN type.
     *
     * @return The BOOLEAN type.
     */
    public static DataType cboolean() {
        return primitiveTypeMap.get(Name.BOOLEAN);
    }

    /**
     * Returns the COUNTER type.
     *
     * @return The COUNTER type.
     */
    public static DataType counter() {
        return primitiveTypeMap.get(Name.COUNTER);
    }

    /**
     * Returns the DECIMAL type.
     *
     * @return The DECIMAL type.
     */
    public static DataType decimal() {
        return primitiveTypeMap.get(Name.DECIMAL);
    }

    /**
     * Returns the DOUBLE type.
     *
     * @return The DOUBLE type.
     */
    public static DataType cdouble() {
        return primitiveTypeMap.get(Name.DOUBLE);
    }

    /**
     * Returns the FLOAT type.
     *
     * @return The FLOAT type.
     */
    public static DataType cfloat() {
        return primitiveTypeMap.get(Name.FLOAT);
    }

    /**
     * Returns the INET type.
     *
     * @return The INET type.
     */
    public static DataType inet() {
        return primitiveTypeMap.get(Name.INET);
    }

    /**
     * Returns the INT type.
     *
     * @return The INT type.
     */
    public static DataType cint() {
        return primitiveTypeMap.get(Name.INT);
    }

    /**
     * Returns the TEXT type.
     *
     * @return The TEXT type.
     */
    public static DataType text() {
        return primitiveTypeMap.get(Name.TEXT);
    }

    /**
     * Returns the TIMESTAMP type.
     *
     * @return The TIMESTAMP type.
     */
    public static DataType timestamp() {
        return primitiveTypeMap.get(Name.TIMESTAMP);
    }

    /**
     * Returns the UUID type.
     *
     * @return The UUID type.
     */
    public static DataType uuid() {
        return primitiveTypeMap.get(Name.UUID);
    }

    /**
     * Returns the VARCHAR type.
     *
     * @return The VARCHAR type.
     */
    public static DataType varchar() {
        return primitiveTypeMap.get(Name.VARCHAR);
    }

    /**
     * Returns the VARINT type.
     *
     * @return The VARINT type.
     */
    public static DataType varint() {
        return primitiveTypeMap.get(Name.VARINT);
    }

    /**
     * Returns the TIMEUUID type.
     *
     * @return The TIMEUUID type.
     */
    public static DataType timeuuid() {
        return primitiveTypeMap.get(Name.TIMEUUID);
    }

    /**
     * Returns the type of lists of {@code elementType} elements.
     *
     * @param elementType the type of the list elements.
     * @return the type of lists of {@code elementType} elements.
     */
    public static DataType list(DataType elementType) {
        // TODO: for list, sets and maps, we could cache them (may or may not be worth it, but since we
        // don't allow nesting of collections, even pregenerating all the lists/sets like we do for
        // primitives wouldn't be very costly)
        return new DataType(Name.LIST, ImmutableList.of(elementType));
    }

    /**
     * Returns the type of sets of {@code elementType} elements.
     *
     * @param elementType the type of the set elements.
     * @return the type of sets of {@code elementType} elements.
     */
    public static DataType set(DataType elementType) {
        return new DataType(Name.SET, ImmutableList.of(elementType));
    }

    /**
     * Returns the type of maps of {@code keyType} to {@code valueType} elements.
     *
     * @param keyType the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of map of {@code keyType} to {@code valueType} elements.
     */
    public static DataType map(DataType keyType, DataType valueType) {
        return new DataType(Name.MAP, ImmutableList.of(keyType, valueType));
    }

    /**
     * Returns a Custom type.
     * <p>
     * A custom type is defined by the name of the class used on the Cassandra
     * side to implement it. Note that the support for custom type by the
     * driver is limited: values of a custom type won't be interpreted by the
     * driver in any way. They will thus have to be set (by {@link BoundStatement#setBytesUnsafe}
     * and retrieved (by {@link Row#getBytesUnsafe}) as raw ByteBuffer.
     * <p>
     * The use of custom types is rarely useful and is thus not encouraged.
     *
     * @param typeClassName the server-side fully qualified class name for the type.
     * @return the custom type for {@code typeClassName}.
     */
    public static DataType custom(String typeClassName) {
        if (typeClassName == null)
            throw new NullPointerException();
        return new DataType(Name.CUSTOM, Collections.<DataType>emptyList(), typeClassName);
    }

    /**
     * Returns the name of that type.
     *
     * @return the name of that type.
     */
    public Name getName() {
        return name;
    }

    /**
     * Returns the type arguments of this type.
     * <p>
     * Note that only the collection types (LIST, MAP, SET) have type
     * arguments. For the other types, this will return an empty list.
     * <p>
     * For the collection types:
     * <ul>
     *   <li>For lists and sets, this method returns one argument, the type of
     *   the elements.</li>
     *   <li>For maps, this method returns two arguments, the first one is the
     *   type of the map keys, the second one is the type of the map
     *   values.</li>
     * </ul>
     *
     * @return an immutable list containing the type arguments of this type.
     */
    public List<DataType> getTypeArguments() {
        return typeArguments;
    }

    /**
     * Returns the server-side class name for a custom type.
     *
     * @return the server-side fully qualified class name for a custom type or
     * {@code null} for any other type.
     */
    public String getCustomTypeClassName() {
        return customClassName;
    }

    /**
     * Returns whether this type is a collection one, i.e. a list, set or map type.
     *
     * @return whether this type is a collection one.
     */
    public boolean isCollection() {
        return name.isCollection();
    }

    /**
     * Returns a set of all the primitive types, where primitive types are
     * defined as the types that don't have type arguments (that is excluding
     * lists, sets, and maps).
     *
     * @return returns a set of all the primitive types.
     */
    public static Set<DataType> allPrimitiveTypes() {
        return primitiveTypeSet;
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(new Object[]{ name, typeArguments, customClassName });
    }

    @Override
    public final boolean equals(Object o) {
        if(!(o instanceof DataType))
            return false;

        DataType d = (DataType)o;
        return (name == d.name || areAliases(name, d.name)) && typeArguments.equals(d.typeArguments) && Objects.equal(customClassName, d.customClassName);
    }

    private boolean areAliases(Name name1, Name name2) {
        return (name1 == Name.TEXT && name2 == Name.VARCHAR) || (name1 == Name.VARCHAR && name2 == Name.TEXT);
    }

    @Override
    public String toString() {
        switch (name) {
            case LIST:
            case SET:
                return String.format("%s<%s>", name, typeArguments.get(0));
            case MAP:
                return String.format("%s<%s, %s>", name, typeArguments.get(0), typeArguments.get(1));
            case CUSTOM:
                return String.format("'%s'", customClassName);
            default:
                return name.toString();
        }
    }
}
