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

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.MarshalException;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Data types supported by cassandra.
 */
public class DataType {

    /**
     * The CQL type name.
     */
    public enum Name {

        ASCII     (String.class),
        BIGINT    (Long.class),
        BLOB      (ByteBuffer.class),
        BOOLEAN   (Boolean.class),
        COUNTER   (Long.class),
        DECIMAL   (BigDecimal.class),
        DOUBLE    (Double.class),
        FLOAT     (Float.class),
        INET      (InetAddress.class),
        INT       (Integer.class),
        TEXT      (String.class),
        TIMESTAMP (Date.class),
        UUID      (UUID.class),
        VARCHAR   (String.class),
        VARINT    (BigInteger.class),
        TIMEUUID  (UUID.class),
        LIST      (List.class),
        SET       (Set.class),
        MAP       (Map.class),
        CUSTOM    (ByteBuffer.class);

        final Class<?> javaType;

        private Name(Class<?> javaType) {
            this.javaType = javaType;
        }

        /**
         * Whether this data type name represent the name of a collection type (e.g. list, set or map).
         *
         * @return whether this data type name represent the name of a collection type.
         */
        public boolean isCollection() {
            switch (this) {
                case LIST:
                case SET:
                case MAP:
                    return true;
            }
            return false;
        }

        /**
         * The java Class corresponding to this CQL type name.
         *
         * The correspondence between CQL types and java ones is as follow:
         * <table>
         *   <tr><th>DataType (CQL)</th><th>Java Class</th></tr>
         *   <tr><td>ASCII         </td><td>String</td></tr>
         *   <tr><td>BIGINT        </td><td>Long</td></tr>
         *   <tr><td>BLOB          </td><td>ByteBuffer</td></tr>
         *   <tr><td>BOOLEAN       </td><td>Boolean</td></tr>
         *   <tr><td>COUNTER       </td><td>Long</td></tr>
         *   <tr><td>CUSTOM        </td><td>ByteBuffer</td></tr>
         *   <tr><td>DECIMAL       </td><td>BigDecimal</td></tr>
         *   <tr><td>DOUBLE        </td><td>Double</td></tr>
         *   <tr><td>FLOAT         </td><td>Float</td></tr>
         *   <tr><td>INET          </td><td>InetAddress</td></tr>
         *   <tr><td>INT           </td><td>Integer</td></tr>
         *   <tr><td>LIST          </td><td>List</td></tr>
         *   <tr><td>MAP           </td><td>Map</td></tr>
         *   <tr><td>SET           </td><td>Set</td></tr>
         *   <tr><td>TEXT          </td><td>String</td></tr>
         *   <tr><td>TIMESTAMP     </td><td>Date</td></tr>
         *   <tr><td>UUID          </td><td>UUID</td></tr>
         *   <tr><td>VARCHAR       </td><td>String</td></tr>
         *   <tr><td>VARINT        </td><td>BigInteger</td></tr>
         *   <tr><td>TIMEUUID      </td><td>UUID</td></tr>
         * </table>
         *
         * @return the java Class corresponding to this CQL type name.
         */
        public Class<?> asJavaClass() {
            return javaType;
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

    private DataType(DataType.Name name, List<DataType> typeArguments, String customClassName) {
        this.name = name;
        this.typeArguments = typeArguments;
        this.customClassName = customClassName;
    }

    /**
     * The ASCII type.
     *
     * @return The ASCII type.
     */
    public static DataType ascii() {
        return primitiveTypeMap.get(Name.ASCII);
    }

    /**
     * The BIGINT type.
     *
     * @return The BIGINT type.
     */
    public static DataType bigint() {
        return primitiveTypeMap.get(Name.BIGINT);
    }

    /**
     * The BLOB type.
     *
     * @return The BLOB type.
     */
    public static DataType blob() {
        return primitiveTypeMap.get(Name.BLOB);
    }

    /**
     * The BOOLEAN type.
     *
     * @return The BOOLEAN type.
     */
    public static DataType cboolean() {
        return primitiveTypeMap.get(Name.BOOLEAN);
    }

    /**
     * The COUNTER type.
     *
     * @return The COUNTER type.
     */
    public static DataType counter() {
        return primitiveTypeMap.get(Name.COUNTER);
    }

    /**
     * The DECIMAL type.
     *
     * @return The DECIMAL type.
     */
    public static DataType decimal() {
        return primitiveTypeMap.get(Name.DECIMAL);
    }

    /**
     * The DOUBLE type.
     *
     * @return The DOUBLE type.
     */
    public static DataType cdouble() {
        return primitiveTypeMap.get(Name.DOUBLE);
    }

    /**
     * The FLOAT type.
     *
     * @return The FLOAT type.
     */
    public static DataType cfloat() {
        return primitiveTypeMap.get(Name.FLOAT);
    }

    /**
     * The INET type.
     *
     * @return The INET type.
     */
    public static DataType inet() {
        return primitiveTypeMap.get(Name.INET);
    }

    /**
     * The INT type.
     *
     * @return The INT type.
     */
    public static DataType cint() {
        return primitiveTypeMap.get(Name.INT);
    }

    /**
     * The TEXT type.
     *
     * @return The TEXT type.
     */
    public static DataType text() {
        return primitiveTypeMap.get(Name.TEXT);
    }

    /**
     * The TIMESTAMP type.
     *
     * @return The TIMESTAMP type.
     */
    public static DataType timestamp() {
        return primitiveTypeMap.get(Name.TIMESTAMP);
    }

    /**
     * The UUID type.
     *
     * @return The UUID type.
     */
    public static DataType uuid() {
        return primitiveTypeMap.get(Name.UUID);
    }

    /**
     * The VARCHAR type.
     *
     * @return The VARCHAR type.
     */
    public static DataType varchar() {
        return primitiveTypeMap.get(Name.VARCHAR);
    }

    /**
     * The VARINT type.
     *
     * @return The VARINT type.
     */
    public static DataType varint() {
        return primitiveTypeMap.get(Name.VARINT);
    }

    /**
     * The TIMEUUID type.
     *
     * @return The TIMEUUID type.
     */
    public static DataType timeuuid() {
        return primitiveTypeMap.get(Name.TIMEUUID);
    }

    /**
     * The type of lists of {@code elementType} elements.
     *
     * @param elementType the type of the list elements.
     * @return the type of lists of {@code elementType} elements.
     */
    public static DataType list(DataType elementType) {
        return new DataType(Name.LIST, ImmutableList.of(elementType));
    }

    /**
     * The type of sets of {@code elementType} elements.
     *
     * @param elementType the type of the set elements.
     * @return the type of sets of {@code elementType} elements.
     */
    public static DataType set(DataType elementType) {
        return new DataType(Name.SET, ImmutableList.of(elementType));
    }

    /**
     * The type of maps of {@code keyType} to {@code valueType} elements.
     *
     * @param keyType the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of map of {@code keyType} to {@code valueType} elements.
     */
    public static DataType map(DataType keyType, DataType valueType) {
        return new DataType(Name.MAP, ImmutableList.of(keyType, valueType));
    }

    /**
     * A Custom type.
     * <p>
     * A custom type is defined by the name of the class used on the Cassandra
     * side to implement it. Note that the support for custom type by the
     * driver is limited: values of a custom type won't be interpreted by the
     * driver in any way.  They will thus be expected (by {@link
     * BoundStatement#setBytesUnsafe)} and returned (by {@link
     * Row#getBytesUnsafe}) as ByteBuffer.
     * <p>
     * The use of custom types is rarely useful and is thus not encouraged.
     * <p>
     * Also note that currently, the class implementing the custom type server
     * side must be present in the driver classpath (this restriction should
     * hopefully lifted at some point).
     *
     * @param typeClassName the server-side class name for the type.
     * @return the custom type for {@code typeClassName}.
     */
    public static DataType custom(String typeClassName) {
        if (typeClassName == null)
            throw new NullPointerException();
        return new DataType(Name.CUSTOM, Collections.<DataType>emptyList(), typeClassName);
    }

    /**
     * The name of that type.
     *
     * @return the name of that type.
     */
    public Name getName() {
        return name;
    }

    /**
     * The type arguments of this type.
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
     * <ul>
     *
     * @return an immutable list containing the type arguments of this type.
     */
    public List<DataType> getTypeArguments() {
        return typeArguments;
    }

    /**
     * The server side class name for a custom types.
     *
     * @return the server side class name for a custom types or {@code null}
     * for any other type.
     */
    public String getCustomTypeClassName() {
        return customClassName;
    }

    /**
     * Parse a string value for the type this object represent, returning its
     * Cassandra binary representation.
     *
     * @param value the value to parse.
     * @return the binary representation of {@code value}.
     *
     * @throws InvalidTypeException if {@code value} is not a valid string
     * representation for this type. Please note that values for custom types
     * can never be parsed and will always return this exception.
     */
    public ByteBuffer parse(String value) {
        if (name == Name.CUSTOM)
            throw new InvalidTypeException(String.format("Cannot parse '%s' as value of custom type of class '%s' "
                                                       + "(values for custom type cannot be parse and must be inputed as bytes directly)", value, customClassName));

        try {
            return Codec.getCodec(this).fromString(value);
        } catch (MarshalException e) {
            throw new InvalidTypeException(String.format("Cannot parse '%s' as a %s value (%s)", value, this, e.getMessage()));
        }
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
     * The java Class corresponding to this type.
     *
     * This is a shortcut for {@code getName().asJavaClass()}.
     *
     * @return the java Class corresponding to this type.
     *
     * @see Name#asJavaClass
     */
    public Class<?> asJavaClass() {
        return getName().asJavaClass();
    }

    /**
     * Returns a set of all the primitive types, where primitive types are
     * defined as the types that don't have type arguments (i.e. excluding
     * lists, sets and maps).
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
        return name == d.name && typeArguments.equals(d.typeArguments) && Objects.equal(customClassName, d.customClassName);
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
