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

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * Collection of (typed) CQL values that can set by index (starting a 0).
 */
public interface SettableByIndexData<T extends SettableByIndexData<T>> {

    /**
     * Sets the {@code i}th value to the provided boolean.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BOOLEAN.
     */
    public T setBool(int i, boolean v);

    /**
     * Set the {@code i}th value to the provided byte.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TINYINT.
     */
    public T setByte(int i, byte v);

    /**
     * Set the {@code i}th value to the provided short.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type SMALLINT.
     */
    public T setShort(int i, short v);

    /**
     * Set the {@code i}th value to the provided integer.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type INT.
     */
    public T setInt(int i, int v);

    /**
     * Sets the {@code i}th value to the provided long.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BIGINT or COUNTER.
     */
    public T setLong(int i, long v);

    /**
     * Set the {@code i}th value to the provided date.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TIMESTAMP.
     * @deprecated deprecated in favor of {@link #setTimestamp(int, Date)}
     */
    @Deprecated
    public T setDate(int i, Date v);

    /**
     * Set the {@code i}th value to the provided date.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TIMESTAMP.
     */
    public T setTimestamp(int i, Date v);

    /**
     * Set the {@code i}th value to the provided date as an int in days since epoch.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DATE.
     */
    public T setDateWithoutTime(int i, DateWithoutTime v);

    /**
     * Set the {@code i}th value to the provided time as a long in nanoseconds since midnight.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type TIME.
     */
    public T setTime(int i, long v);

    /**
     * Sets the {@code i}th value to the provided float.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type FLOAT.
     */
    public T setFloat(int i, float v);

    /**
     * Sets the {@code i}th value to the provided double.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DOUBLE.
     */
    public T setDouble(int i, double v);

    /**
     * Sets the {@code i}th value to the provided string.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is of neither of the
     * following types: VARCHAR, TEXT or ASCII.
     */
    public T setString(int i, String v);

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * This method validate that the type of the column set is BLOB. If you
     * want to insert manually serialized data into columns of another type,
     * use {@link #setBytesUnsafe} instead.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type BLOB.
     */
    public T setBytes(int i, ByteBuffer v);

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * Contrary to {@link #setBytes}, this method does not check the
     * type of the column set. If you insert data that is not compatible with
     * the type of the column, you will get an {@code InvalidQueryException} at
     * execute time.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public T setBytesUnsafe(int i, ByteBuffer v);

    /**
     * Sets the {@code i}th value to the provided big integer.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type VARINT.
     */
    public T setVarint(int i, BigInteger v);

    /**
     * Sets the {@code i}th value to the provided big decimal.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type DECIMAL.
     */
    public T setDecimal(int i, BigDecimal v);

    /**
     * Sets the {@code i}th value to the provided UUID.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type UUID or
     * TIMEUUID, or if value {@code i} is of type TIMEUUID but {@code v} is
     * not a type 1 UUID.
     */
    public T setUUID(int i, UUID v);

    /**
     * Sets the {@code i}th value to the provided inet address.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not of type INET.
     */
    public T setInet(int i, InetAddress v);

    /**
     * Sets the {@code i}th value to the provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(int i, List<E> v);

    /**
     * Sets the {@code i}th value to the provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a map type or
     * if the elements (keys or values) of {@code v} are not of the type of the
     * elements of column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(int i, Map<K, V> v);

    /**
     * Sets the {@code i}th value to the provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a set type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(int i, Set<E> v);

    /**
     * Sets the {@code i}th value to the provided UDT value.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a UDT value or if its definition
     * does not correspond to the one of {@code v}.
     */
    public T setUDTValue(int i, UDTValue v);

    /**
     * Sets the {@code i}th value to the provided tuple value.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a tuple value or if its types
     * do not correspond to the ones of {@code v}.
     */
    public T setTupleValue(int i, TupleValue v);

    /**
     * Sets the {@code i}th value to {@code null}.
     * <p>
     * This is mainly intended for CQL types which map to native Java types.
     *
     * @param i the index of the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public T setToNull(int i);
}