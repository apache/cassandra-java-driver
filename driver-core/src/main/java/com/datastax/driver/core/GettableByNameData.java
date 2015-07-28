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

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Collection of (typed) CQL values that can be retrieved by name.
 */
public interface GettableByNameData {

    /**
     * Returns whether the value for {@code name} is NULL.
     *
     * @param name the name to check.
     * @return whether the value for {@code name} is NULL.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     */
    public boolean isNull(String name);

    /**
     * Returns the value for {@code name} as a boolean.
     *
     * @param name the name to retrieve.
     * @return the boolean value for {@code name}. If the value is NULL,
     * {@code false} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type BOOLEAN.
     */
    public boolean getBool(String name);

    /**
     * Returns the value for {@code name} as a byte.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a byte. If the value is NULL,
     * {@code 0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type TINYINT.
     */
    public byte getByte(String name);

    /**
     * Returns the value for {@code name} as a short.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a short. If the value is NULL,
     * {@code 0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type SMALLINT.
     */
    public short getShort(String name);

    /**
     * Returns the value for {@code name} as an integer.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as an integer. If the value is NULL,
     * {@code 0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type INT.
     */
    public int getInt(String name);

    /**
     * Returns the value for {@code name} as a long.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a long. If the value is NULL,
     * {@code 0L} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BIGINT or COUNTER.
     */
    public long getLong(String name);

    /**
     * Returns the value for {@code name} as a date.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a date. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type TIMESTAMP.
     */
    public Date getTimestamp(String name);

    /**
     * Returns the value for {@code name} as a date (without time).
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a date. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type DATE.
     */
    public LocalDate getDate(String name);

    /**
     * Returns the value for {@code name} as a long in nanoseconds since midnight.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a long. If the value is NULL,
     * {@code 0L} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type TIME.
     */
    public long getTime(String name);

    /**
     * Returns the value for {@code name} as a float.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a float. If the value is NULL,
     * {@code 0.0f} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type FLOAT.
     */
    public float getFloat(String name);

    /**
     * Returns the value for {@code name} as a double.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a double. If the value is NULL,
     * {@code 0.0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type DOUBLE.
     */
    public double getDouble(String name);

    /**
     * Returns the value for {@code name} as a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a ByteBuffer. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     */
    public ByteBuffer getBytesUnsafe(String name);

    /**
     * Returns the value for {@code name} as a byte array.
     * <p>
     * Note that this method validate that the column is of type BLOB. If you want to retrieve
     * the bytes for any type, use {@link #getBytesUnsafe(String)} instead.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a byte array. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code i} type is not of type BLOB.
     */
    public ByteBuffer getBytes(String name);

    /**
     * Returns the value for {@code name} as a string.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a string. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    public String getString(String name);

    /**
     * Returns the value for {@code name} as a variable length integer.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a variable length integer.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type VARINT.
     */
    public BigInteger getVarint(String name);

    /**
     * Returns the value for {@code name} as a variable length decimal.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a variable length decimal.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type DECIMAL.
     */
    public BigDecimal getDecimal(String name);

    /**
     * Returns the value for {@code name} as a UUID.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type
     * UUID or TIMEUUID.
     */
    public UUID getUUID(String name);

    /**
     * Returns the value for {@code name} as an InetAddress.
     *
     * @param name the name to retrieve.
     * @return the value for {@code name} as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not of type
     * INET.
     */
    public InetAddress getInet(String name);

    /**
     * Returns the value for {@code name} as a list.
     * <p>
     * If the type of the elements is generic, use {@link #getList(String, TypeToken)}.
     *
     * @param name the name to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a list or if its
     * elements are not of class {@code T}.
     */
    public <T> List<T> getList(String name, Class<T> elementsClass);

    /**
     * Returns the value for {@code name} as a list.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code List<List<String>> l = row.getList("theColumn", new TypeToken<List<String>>() {});}
     * </pre>
     *
     * @param name the name to retrieve.
     * @param elementsType the type for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a list or if its
     * elements are not of class {@code T}.
     */
    public <T> List<T> getList(String name, TypeToken<T> elementsType);

    /**
     * Returns the value for {@code name} as a set.
     * <p>
     * If the type of the elements is generic, use {@link #getSet(String, TypeToken)}.
     *
     * @param name the name to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a set or if its
     * elements are not of class {@code T}.
     */
    public <T> Set<T> getSet(String name, Class<T> elementsClass);

    /**
     * Returns the value for {@code name} as a set.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code Set<List<String>> l = row.getSet("theColumn", new TypeToken<List<String>>() {});}
     * </pre>
     *
     * @param name the name to retrieve.
     * @param elementsType the type for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a set or if its
     * elements are not of class {@code T}.
     */
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType);

    /**
     * Returns the value for {@code name} as a map.
     * <p>
     * If the type of the keys and/or values is generic, use {@link #getMap(String, TypeToken, TypeToken)}.
     *
     * @param name the name to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of {@code name} as a map of
     * {@code K} to {@code V} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a map, if its
     * keys are not of class {@code K} or if its values are not of
     * class {@code V}.
     */
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Returns the value for {@code name} as a map.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code Map<Int, List<String>> l = row.getMap("theColumn", TypeToken.of(Integer.class), new TypeToken<List<String>>() {});}
     * </pre>
     *
     * @param name the name to retrieve.
     * @param keysType the class for the keys of the map to retrieve.
     * @param valuesType the class for the values of the map to retrieve.
     * @return the value of {@code name} as a map of
     * {@code K} to {@code V} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code name} is not a map, if its
     * keys are not of class {@code K} or if its values are not of
     * class {@code V}.
     */
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Return the value for {@code name} as a UDT value.
     *
     * @param name the name to retrieve.
     * @return the value of {@code name} as a UDT value. If the value is NULL,
     * then {@code null} will be returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code i} is not a UDT value.
     */
    public UDTValue getUDTValue(String name);

    /**
     * Return the value for {@code name} as a tuple value.
     *
     * @param name the name to retrieve.
     * @return the value of {@code name} as a tuple value. If the value is NULL,
     * then {@code null} will be returned.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     * @throws InvalidTypeException if value {@code i} is not a tuple value.
     */
    public TupleValue getTupleValue(String name);

    /**
     * Returns the value for {@code name} as the Java type matching its CQL type.
     * <p>
     * This method uses the default codec for the underlying CQL type
     * to perform deserialization, and is safe to be used
     * <em>as long as only default codecs are in use</em>.
     * If a second, custom codec for the same CQL type is registered, which one will
     * be used is unspecified; in such cases, it is preferable to use
     * the more deterministic methods {@link #get(String, Class)} or {@link #get(String, TypeToken)} instead.
     *
     * @param name the name to retrieve.
     * @return the value of the {@code i}th value as the Java type matching its CQL type.
     * If the value is NULL and is a simple type, UDT or tuple, {@code null} is returned.
     * If it is NULL and is a collection type, an empty (immutable) collection is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    Object getObject(String name);

    /**
     * Returns the value for {@code name} converted to the given Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for for the underlying CQL type and {@code targetClass} must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     * <p>
     * This method should be used instead of {@link #getObject(String)} in cases
     * where more than one codec is registered for the same CQL type; specifying the Java class
     * allows the {@link CodecRegistry} to narrow down the search and return only an exactly-matching codec (if any),
     * thus avoiding any risk of ambiguity.
     *
     * @param name the name to retrieve.
     * @param targetClass The Java type the value should be converted to.
     * @return the value for {@code name} value converted to the given Java type.
     * If the CQL value is {@code NULL}, then {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws com.datastax.driver.core.exceptions.CodecNotFoundException
     * if no {@link TypeCodec} instance for {@code targetClass} could be found
     * by the {@link CodecRegistry} currently in use.
     */
    <T> T get(String name, Class<T> targetClass);

    /**
     * Returns the value for {@code name} converted to the given Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for for the underlying CQL type and {@code targetType} must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     * <p>
     * This method should be used instead of {@link #getObject(String)} in cases
     * where more than one codec is registered for the same CQL type; specifying the Java class
     * allows the {@link CodecRegistry} to narrow down the search and return only an exactly-matching codec (if any),
     * thus avoiding any risk of ambiguity.
     *
     * @param name the name to retrieve.
     * @param targetType The Java type the value should be converted to.
     * @return the value for {@code name} value converted to the given Java type.
     * If the CQL value is {@code NULL}, then {@code null} is returned.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws com.datastax.driver.core.exceptions.CodecNotFoundException
     * if no {@link TypeCodec} instance for {@code targetType} could be found
     * by the {@link CodecRegistry} currently in use.
     */
    <T> T get(String name, TypeToken<T> targetType);

    /**
     * Returns the value for {@code name} converted using the given {@link TypeCodec}.
     * <p>
     * Note that this method allows to entirely bypass the {@link CodecRegistry} currently in use
     * and forces the driver to use the given codec instead.
     * <p>
     * It is the caller's responsibility to ensure that the given codec {@link TypeCodec#accepts(DataType) accepts}
     * the underlying CQL type; failing to do so may result in {@link InvalidTypeException}s being thrown.
     *
     * @param name the name to retrieve.
     * @param codec The {@link TypeCodec} to use to deserialize the value; may not be {@code null}.
     * @return the value of the {@code i}th value converted using the given {@link TypeCodec}.
     * @throws InvalidTypeException if the given codec does not {@link TypeCodec#accepts(DataType) accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <T> T get(String name, TypeCodec<T> codec);

}
