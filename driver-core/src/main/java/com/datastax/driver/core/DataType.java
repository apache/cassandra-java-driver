package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.marshal.MarshalException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Data types supported by cassandra.
 */
public class DataType {

    public enum Name {

        ASCII,
        BIGINT,
        BLOB,
        BOOLEAN,
        COUNTER,
        DECIMAL,
        DOUBLE,
        FLOAT,
        INET,
        INT,
        TEXT,
        TIMESTAMP,
        UUID,
        VARCHAR,
        VARINT,
        TIMEUUID,
        LIST,
        SET,
        MAP;

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

        @Override
        public String toString() {
            return super.toString().toLowerCase();
        }
    }

    private final DataType.Name name;
    private final List<DataType> typeArguments;

    private static final Map<Name, DataType> primitiveTypeMap = new EnumMap<Name, DataType>(Name.class);
    static {
        for (Name name : Name.values()) {
            if (!name.isCollection())
                primitiveTypeMap.put(name, new DataType(name, Collections.<DataType>emptyList()));
        }
    }
    private static final Set<DataType> primitveTypeSet = ImmutableSet.copyOf(primitiveTypeMap.values());

    private DataType(DataType.Name name, List<DataType> typeArguments) {
        this.name = name;
        this.typeArguments = typeArguments;
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
     * Parse a string value for the type this object represent, returning its
     * Cassandra binary representation.
     *
     * @param value the value to parse.
     * @return the binary representation of {@code value}.
     *
     * @throws InvalidTypeException if {@code value} is not a valid string
     * representation for this type.
     */
    public ByteBuffer parse(String value) {
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
     * Returns a set of all the primitive types, where primitive types are
     * defined as the types that don't have type arguments (i.e. excluding
     * lists, sets and maps).
     *
     * @return returns a set of all the primitive types.
     */
    public static Set<DataType> allPrimitiveTypes() {
        return primitveTypeSet;
    }

    @Override
    public String toString() {
        switch (name) {
            case LIST:
            case SET:
                return name + "<" + typeArguments.get(0) + ">";
            case MAP:
                return name + "<" + typeArguments.get(0) + ", " + typeArguments.get(1) + ">";
            default:
                return name.toString();
        }
    }
}
