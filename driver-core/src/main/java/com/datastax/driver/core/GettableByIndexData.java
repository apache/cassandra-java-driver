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
     * @deprecated deprecated in favor of {@link #getTimestamp(int)}
     */
    @Deprecated
    public Date getDate(int i);

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
     * Returns the {@code i}th value as a date as an int in days since epoch.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @return the value of the {@code i}th element as an int. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DATE.
     */
    public DateWithoutTime getDateWithoutTime(int i);

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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type of the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code T} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsType the type for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code T} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of
     * {@code K} to {@code V} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysType the type for the keys of the map to retrieve.
     * @param valuesType the type for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of
     * {@code K} to {@code V} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
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
     *
     * @param i the index to retrieve.
     * @return the value of the {@code i}th value as the Java type matching its CQL type.
     * If the value is NULL and is a simple type, UDT or tuple, {@code null} is returned.
     * If it is NULL and is a collection type, an empty (immutable) collection is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public Object getObject(int i);
}