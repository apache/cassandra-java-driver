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
 * Collection of (typed) CQL values that can be retrieved by index (starting a 0).
 */
public interface GettableByIndexData {

    /**
     * Returns whether the {@code i}th value is NULL.
     *
     * @param i the index ({@code 0 <= i < size()}) of the value to check.
     * @return whether the {@code i}th value is NULL.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for
     * this object.
     */
    public boolean isNull(int i);

    /**
     * Returns the {@code i}th value as a boolean.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the boolean value of the {@code i}th element. If the
     * value is NULL, {@code false} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BOOLEAN.
     */
    public boolean getBool(int i);

    /**
     * Returns the {@code i}th value as a byte.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a byte. If the
     * value is NULL, {@code 0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TINYINT.
     */
    public byte getByte(int i);

    /**
     * Returns the {@code i}th value as a short.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a short. If the
     * value is NULL, {@code 0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type SMALLINT.
     */
    public short getShort(int i);

    /**
     * Returns the {@code i}th value as an integer.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an integer. If the
     * value is NULL, {@code 0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type INT.
     */
    public int getInt(int i);

    /**
     * Returns the {@code i}th value as a long.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a long. If the
     * value is NULL, {@code 0L} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BIGINT or COUNTER.
     */
    public long getLong(int i);

    /**
     * Returns the {@code i}th value as a date.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a data. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TIMESTAMP.
     */
    public Date getTimestamp(int i);

    /**
     * Returns the {@code i}th value as a date (without time).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an date. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DATE.
     */
    public LocalDate getDate(int i);

    /**
     * Returns the {@code i}th value as a long in nanoseconds since midnight.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a long. If the
     * value is NULL, {@code 0L} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TIME.
     */
    public long getTime(int i);

    /**
     * Returns the {@code i}th value as a float.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a float. If the
     * value is NULL, {@code 0.0f} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type FLOAT.
     */
    public float getFloat(int i);

    /**
     * Returns the {@code i}th value as a double.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a double. If the
     * value is NULL, {@code 0.0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DOUBLE.
     */
    public double getDouble(int i);

    /**
     * Returns the {@code i}th value as a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a ByteBuffer. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public ByteBuffer getBytesUnsafe(int i);

    /**
     * Returns the {@code i}th value as a byte array.
     * <p>
     * Note that this method validate that the column is of type BLOB. If you want to retrieve
     * the bytes for any type, use {@link #getBytesUnsafe(int)} instead.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a byte array. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} type is not of type BLOB.
     */
    public ByteBuffer getBytes(int i);

    /**
     * Returns the {@code i}th value as a string.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a string. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    public String getString(int i);

    /**
     * Returns the {@code i}th value as a variable length integer.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a variable
     * length integer. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type VARINT.
     */
    public BigInteger getVarint(int i);

    /**
     * Returns the {@code i}th value as a variable length decimal.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a variable
     * length decimal. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DECIMAL.
     */
    public BigDecimal getDecimal(int i);

    /**
     * Returns the {@code i}th value as a UUID.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type UUID
     * or TIMEUUID.
     */
    public UUID getUUID(int i);

    /**
     * Returns the {@code i}th value as an InetAddress.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type INET.
     */
    public InetAddress getInet(int i);

    /**
     * Returns the {@code i}th value as a list.
     * <p>
     * If the type of the elements is generic, use {@link #getList(int, TypeToken)}.
     * <p>
     * Implementation note: the actual {@link List} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list or if its
     * elements are not of class {@code T}.
     */
    public <T> List<T> getList(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a list.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code List<List<String>> l = row.getList(1, new TypeToken<List<String>>() {});}
     * </pre>
     * <p>
     * Implementation note: the actual {@link List} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type of the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list or if its
     * elements are not of class {@code T}.
     */
    public <T> List<T> getList(int i, TypeToken<T> elementsType);

    /**
     * Returns the {@code i}th value as a set.
     * <p>
     * If the type of the elements is generic, use {@link #getSet(int, TypeToken)}.
     * <p>
     * Implementation note: the actual {@link Set} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a set or if its
     * elements are not of class {@code T}.
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a set.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code Set<List<String>> l = row.getSet(1, new TypeToken<List<String>>() {});}
     * </pre>
     * <p>
     * Implementation note: the actual {@link Set} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a set or if its
     * elements are not of class {@code T}.
     */
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType);

    /**
     * Returns the {@code i}th value as a map.
     * <p>
     * If the type of the keys and/or values is generic, use {@link #getMap(int, TypeToken, TypeToken)}.
     * <p>
     * Implementation note: the actual {@link Map} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of
     * {@code K} to {@code V} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a map, if its
     * keys are not of class {@code K} or if its values are not of
     * class {@code V}.
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass);


    /**
     * Returns the {@code i}th value as a map.
     * <p>
     * Use this variant with nested collections, which produce a generic element type:
     * <pre>
     * {@code Map<Int, List<String>> l = row.getMap(1, TypeToken.of(Integer.class), new TypeToken<List<String>>() {});}
     * </pre>
     * <p>
     * Implementation note: the actual {@link Map} implementation will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent. By default, the driver will return mutable instances, and
     * a CQL {@code NULL} will mapped to an empty collection (note that Cassandra
     * makes no distinction between {@code NULL} and an empty collection).
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysType the type for the keys of the map to retrieve.
     * @param valuesType the type for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of
     * {@code K} to {@code V} objects.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a map, if its
     * keys are not of class {@code K} or if its values are not of
     * class {@code V}.
     */
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Return the {@code i}th value as a UDT value.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a UDT value. If the value is NULL,
     * then {@code null} will be returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a UDT value.
     */
    public UDTValue getUDTValue(int i);

    /**
     * Return the {@code i}th value as a tuple value.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as a tuple value. If the value is NULL,
     * then {@code null} will be returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a tuple value.
     */
    public TupleValue getTupleValue(int i);

    /**
     * Returns the {@code i}th value as the Java type matching its CQL type.
     * <p>
     * This method uses the default codec for the underlying CQL type
     * to perform deserialization, and is safe to be used
     * <em>as long as only default codecs are in use</em>.
     * If a second, custom codec for the same CQL type is registered, which one will
     * be used is unspecified; in such cases, it is preferable to use
     * the more deterministic methods {@link #get(int, Class)} or {@link #get(int, TypeToken)} instead.
     * <p>
     * Implementation note: the actual object returned by this method will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent; by default, a CQL {@code NULL} value translates to {@code null} for
     * simple types, UDTs and tuples, and to empty collections for all CQL collection types.
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value as the Java type matching its CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public Object getObject(int i);

    /**
     * Returns the {@code i}th value converted to the given Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and {@code targetClass} must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     * <p>
     * This method should be used instead of {@link #getObject(int)} in cases
     * where more than one codec is registered for the same CQL type; specifying the Java class
     * allows the {@link CodecRegistry} to narrow down the search and return only an exactly-matching codec (if any),
     * thus avoiding any risk of ambiguity.
     * <p>
     * Implementation note: the actual object returned by this method will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent; by default, a CQL {@code NULL} value translates to {@code null} for
     * simple CQL types, UDTs and tuples, and to empty collections for all CQL collection types.
     *
     * @param i the index to retrieve.
     * @param targetClass The Java type the value should be converted to.
     * @return the value of the {@code i}th value converted to the given Java type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws com.datastax.driver.core.exceptions.CodecNotFoundException
     * if no {@link TypeCodec} instance for {@code targetClass} could be found
     * by the {@link CodecRegistry} currently in use.
     */
    <T> T get(int i, Class<T> targetClass);

    /**
     * Returns the {@code i}th value converted to the given Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and {@code targetType} must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     * <p>
     * This method should be used instead of {@link #getObject(int)} in cases
     * where more than one codec is registered for the same CQL type; specifying the Java class
     * allows the {@link CodecRegistry} to narrow down the search and return only an exactly-matching codec (if any),
     * thus avoiding any risk of ambiguity.
     * <p>
     * Implementation note: the actual object returned by this method will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent; by default, a CQL {@code NULL} value translates to {@code null} for
     * simple CQL types, UDTs and tuples, and to empty collections for all CQL collection types.
     *
     * @param i the index to retrieve.
     * @param targetType The Java type the value should be converted to.
     * @return the value of the {@code i}th value converted to the given Java type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws com.datastax.driver.core.exceptions.CodecNotFoundException
     * if no {@link TypeCodec} instance for {@code targetType} could be found
     * by the {@link CodecRegistry} currently in use.
     */
    <T> T get(int i, TypeToken<T> targetType);

    /**
     * Returns the {@code i}th value converted using the given {@link TypeCodec}.
     * <p>
     * Note that this method allows to entirely bypass the {@link CodecRegistry} currently in use
     * and forces the driver to use the given codec instead.
     * <p>
     * It is the caller's responsibility to ensure that the given codec {@link TypeCodec#accepts(DataType) accepts}
     * the underlying CQL type; failing to do so may result in {@link InvalidTypeException}s being thrown.
     * <p>
     * Implementation note: the actual object returned by this method will depend
     * on the {@link TypeCodec codec} being used; therefore, callers should
     * make no assumptions concerning its mutability nor its thread-safety.
     * Furthermore, the behavior of this method in respect to CQL {@code NULL} values is also
     * codec-dependent; by default, a CQL {@code NULL} value translates to {@code null} for
     * simple CQL types, UDTs and tuples, and to empty collections for all CQL collection types.
     *
     * @param i the index to retrieve.
     * @param codec The {@link TypeCodec} to use to deserialize the value; may not be {@code null}.
     * @return the value of the {@code i}th value converted using the given {@link TypeCodec}.
     * @throws InvalidTypeException if the given codec does not {@link TypeCodec#accepts(DataType) accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <T> T get(int i, TypeCodec<T> codec);

}
