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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jboss.netty.buffer.ChannelBuffer;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Data types supported by cassandra.
 */
public abstract class DataType {

    /**
     * The CQL type name.
     */
    public enum Name {

        ASCII     (1,  String.class),
        BIGINT    (2,  Long.class),
        BLOB      (3,  ByteBuffer.class),
        BOOLEAN   (4,  Boolean.class),
        COUNTER   (5,  Long.class),
        DECIMAL   (6,  BigDecimal.class),
        DOUBLE    (7,  Double.class),
        FLOAT     (8,  Float.class),
        INET      (16, InetAddress.class),
        INT       (9,  Integer.class),
        TEXT      (10, String.class),
        TIMESTAMP (11, Date.class),
        UUID      (12, UUID.class),
        VARCHAR   (13, String.class),
        VARINT    (14, BigInteger.class),
        TIMEUUID  (15, UUID.class),
        LIST      (32, List.class),
        SET       (34, Set.class),
        MAP       (33, Map.class),
        CUSTOM    (0,  ByteBuffer.class);

        final int protocolId;
        final Class<?> javaType;

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

        private Name(int protocolId, Class<?> javaType) {
            this.protocolId = protocolId;
            this.javaType = javaType;
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

        /**
         * Returns the Java Class corresponding to this CQL type name.
         *
         * The correspondence between CQL types and java ones is as follow:
         * <table>
         *   <caption>DataType to Java class correspondence</caption>
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

    protected final DataType.Name name;
    protected final TypeCodec<?> codec;

    private static final Map<Name, DataType> primitiveTypeMap = new EnumMap<Name, DataType>(Name.class);
    static {
        for (Name name : Name.values()) {
            if (!name.isCollection() && name != Name.CUSTOM)
                primitiveTypeMap.put(name, new DataType.Native(name, TypeCodec.createFor(name)));
        }
    }
    private static final Set<DataType> primitiveTypeSet = ImmutableSet.copyOf(primitiveTypeMap.values());

    protected DataType(DataType.Name name, TypeCodec<?> codec) {
        this.name = name;
        this.codec = codec;
    }

    static DataType decode(ChannelBuffer buffer) {
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

    @SuppressWarnings("unchecked")
    TypeCodec<Object> codec() {
        return (TypeCodec<Object>)codec;
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
        return new DataType.Collection(Name.LIST, TypeCodec.listOf(elementType), ImmutableList.of(elementType));
    }

    /**
     * Returns the type of sets of {@code elementType} elements.
     *
     * @param elementType the type of the set elements.
     * @return the type of sets of {@code elementType} elements.
     */
    public static DataType set(DataType elementType) {
        return new DataType.Collection(Name.SET, TypeCodec.setOf(elementType), ImmutableList.of(elementType));
    }

    /**
     * Returns the type of maps of {@code keyType} to {@code valueType} elements.
     *
     * @param keyType the type of the map keys.
     * @param valueType the type of the map values.
     * @return the type of map of {@code keyType} to {@code valueType} elements.
     */
    public static DataType map(DataType keyType, DataType valueType) {
        return new DataType.Collection(Name.MAP, TypeCodec.mapOf(keyType, valueType), ImmutableList.of(keyType, valueType));
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
        return new DataType.Custom(Name.CUSTOM, TypeCodec.createFor(Name.CUSTOM), typeClassName);
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
        return Collections.<DataType>emptyList();
    }

    /**
     * Returns the server-side class name for a custom type.
     *
     * @return the server-side fully qualified class name for a custom type or
     * {@code null} for any other type.
     */
    public String getCustomTypeClassName() {
        return null;
    }

    /**
     * Parses a string value for the type this object represent, returning its
     * Cassandra binary representation.
     * <p>
     * Please note that currently, parsing collections is not supported and will
     * throw an {@code InvalidTypeException}.
     *
     * @param value the value to parse.
     * @return the binary representation of {@code value}.
     *
     * @throws InvalidTypeException if {@code value} is not a valid string
     * representation for this type. Please note that values for custom types
     * can never be parsed and will always return this exception.
     */
    public abstract ByteBuffer parse(String value);

    /**
     * Returns whether this type is a collection one, i.e. a list, set or map type.
     *
     * @return whether this type is a collection one.
     */
    public boolean isCollection() {
        return name.isCollection();
    }

    /**
     * Returns the Java Class corresponding to this type.
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
     * defined as the types that don't have type arguments (that is excluding
     * lists, sets, and maps).
     *
     * @return returns a set of all the primitive types.
     */
    public static Set<DataType> allPrimitiveTypes() {
        return primitiveTypeSet;
    }

    /**
     * Serialize a value of this type to bytes.
     * <p>
     * The actual format of the resulting bytes will correspond to the
     * Cassandra encoding for this type.
     *
     * @param value the value to serialize.
     * @return the value serialized, or {@code null} if {@code value} is null.
     *
     * @throws InvalidTypeException if {@code value} is not a valid object
     * for this {@code DataType}.
     */
    public ByteBuffer serialize(Object value) {
        Class<?> providedClass = value.getClass();
        Class<?> expectedClass = asJavaClass();
        if (!expectedClass.isAssignableFrom(providedClass))
            throw new InvalidTypeException(String.format("Invalid value for CQL type %s, expecting %s but %s provided", toString(), expectedClass, providedClass));

        try {
            return codec().serialize(value);
        } catch (ClassCastException e) {
            // With collections, the element type has not been checked, so it can throw
            throw new InvalidTypeException("Invalid type for collection element: " + e.getMessage());
        }
    }

    /**
     * Deserialize a value of this type from the provided bytes.
     * <p>
     * The format of {@code bytes} must correspond to the Cassandra
     * encoding for this type.
     *
     * @param bytes bytes holding the value to deserialize.
     * @return the deserialized value (of class {@code this.asJavaClass()}).
     * Will return {@code null} if either {@code bytes} is {@code null} or if
     * {@code bytes.remaining() == 0} and this type has no value corresponding
     * to an empty byte buffer (the latter somewhat strange behavior is due to
     * the fact that for historical/technical reason, Cassandra types always
     * accept empty byte buffer as valid value of those type, and so we avoid
     * throwing an exception in that case. It is however highly discouraged to
     * store empty byte buffers for types for which it doesn't make sense, so
     * this implementation can generally be ignored).
     *
     * @throws InvalidTypeException if {@code bytes} is not a valid
     * encoding of an object of this {@code DataType}.
     */
    public Object deserialize(ByteBuffer bytes) {
        return codec().deserialize(bytes);
    }

    /**
     * Serialize an object based on its java class.
     * <p>
     * This is equivalent to {@link #serialize} but with the difference that
     * the actual {@code DataType} of the resulting value is inferred from the
     * java class of {@code value}. The correspondence between CQL {@code DataType}
     * and java class used is the one induced by the method {@link Name#asJavaClass}.
     * Note that if you know the {@code DataType} of {@code value}, you should use
     * the {@link #serialize} method instead as it is going to be faster.
     *
     * @param value the value to serialize.
     * @return the value serialized, or {@code null} if {@code value} is null.
     *
     * @throws IllegalArgumentException if {@code value} is not of a type
     * corresponding to a CQL3 type, i.e. is not a Class that could be returned
     * by {@link DataType#asJavaClass}.
     */
    public static ByteBuffer serializeValue(Object value) {
        if (value == null)
            return null;

        DataType dt = TypeCodec.getDataTypeFor(value);
        if (dt == null)
            throw new IllegalArgumentException(String.format("Value of type %s does not correspond to any CQL3 type", value.getClass()));

        try {
            return dt.serialize(value);
        } catch (InvalidTypeException e) {
            // In theory we couldn't get that if getDataTypeFor does his job correctly,
            // but there is no point in sending an exception that the user won't expect if we're
            // wrong on that.
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    private static class Native extends DataType {
        private Native(DataType.Name name, TypeCodec<?> codec) {
            super(name, codec);
        }

        @Override
        public ByteBuffer parse(String value) {
            return codec().serialize(codec.parse(value));
        }

        @Override
        public final int hashCode() {
            return name.hashCode();
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof DataType.Native))
                return false;

            return name == ((DataType.Native)o).name;
        }

        @Override
        public String toString() {
            return name.toString();
        }
    }

    private static class Collection extends DataType {

        private final List<DataType> typeArguments;

        private Collection(DataType.Name name, TypeCodec<?> codec, List<DataType> typeArguments) {
            super(name, codec);
            this.typeArguments = typeArguments;
        }

        @Override
        public List<DataType> getTypeArguments() {
            return typeArguments;
        }

        @Override
        public ByteBuffer parse(String value) {
            throw new InvalidTypeException(String.format("Cannot parse value as %s, parsing collections is not currently supported", name));
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{ name, typeArguments });
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof DataType.Collection))
                return false;

            DataType.Collection d = (DataType.Collection)o;
            return name == d.name && typeArguments.equals(d.typeArguments);
        }

        @Override
        public String toString() {
            if (name == Name.MAP)
                return String.format("%s<%s, %s>", name, typeArguments.get(0), typeArguments.get(1));
            else
                return String.format("%s<%s>", name, typeArguments.get(0));
        }
    }

    private static class Custom extends DataType {

        private final String customClassName;

        private Custom(DataType.Name name, TypeCodec<?> codec, String className) {
            super(name, codec);
            this.customClassName = className;
        }

        @Override
        public String getCustomTypeClassName() {
            return customClassName;
        }

        @Override
        public ByteBuffer parse(String value) {
            throw new InvalidTypeException(String.format("Cannot parse '%s' as value of custom type of class '%s' "
                        + "(values for custom type cannot be parse and must be inputted as bytes directly)", value, customClassName));
        }

        @Override
        public final int hashCode() {
            return Arrays.hashCode(new Object[]{ name, customClassName });
        }

        @Override
        public final boolean equals(Object o) {
            if(!(o instanceof DataType.Custom))
                return false;

            DataType.Custom d = (DataType.Custom)o;
            return name == d.name && Objects.equal(customClassName, d.customClassName);
        }

        @Override
        public String toString() {
            return String.format("'%s'", customClassName);
        }
    }
}
