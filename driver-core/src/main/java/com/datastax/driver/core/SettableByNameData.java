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
 * Collection of (typed) CQL values that can set by name.
 */
public interface SettableByNameData<T extends SettableData<T>> {

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided boolean.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type BOOLEAN.
     */
    public T setBool(String name, boolean v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type TINYINT.
     */
    public T setByte(String name, byte v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided short.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type SMALLINT.
     */
    public T setShort(String name, short v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided integer.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type INT.
     */
    public T setInt(String name, int v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided long.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type BIGINT or COUNTER.
     */
    public T setLong(String name, long v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided date.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type TIMESTAMP.
     * @deprecated deprecated in favor of {@link #setTimestamp(String, Date)}
     */
    @Deprecated
    public T setDate(String name, Date v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided date.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type TIMESTAMP.
     */
    public T setTimestamp(String name, Date v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided date as an int in days since epoch.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type DATE.
     */
    public T setDateWithoutTime(String name, DateWithoutTime v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided time as a long in nanoseconds since midnight.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type TIME.
     */
    public T setTime(String name, long v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided float.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type FLOAT.
     */
    public T setFloat(String name, float v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided double.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type DOUBLE.
     */
    public T setDouble(String name, double v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided string.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * of neither of the following types: VARCHAR, TEXT or ASCII.
     */
    public T setString(String name, String v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte buffer.
     *
     * This method validate that the type of the column set is BLOB. If you
     * want to insert manually serialized data into columns of another type,
     * use {@link #setBytesUnsafe} instead.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is not of type BLOB.
     */
    public T setBytes(String name, ByteBuffer v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte buffer.
     *
     * Contrary to {@link #setBytes}, this method does not check the
     * type of the column set. If you insert data that is not compatible with
     * the type of the column, you will get an {@code InvalidQueryException} at
     * execute time.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    public T setBytesUnsafe(String name, ByteBuffer v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided big integer.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type VARINT.
     */
    public T setVarint(String name, BigInteger v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided big decimal.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type DECIMAL.
     */
    public T setDecimal(String name, BigDecimal v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided UUID.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type UUID or TIMEUUID, or if value {@code name} is of type
     * TIMEUUID but {@code v} is not a type 1 UUID.
     */
    public T setUUID(String name, UUID v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided inet address.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type INET.
     */
    public T setInet(String name, InetAddress v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a list type or if the elements of {@code v} are not of the type of
     * the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(String name, List<E> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(String name, Map<K, V> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements of {@code v} are not of the type of
     * the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(String name, Set<E> v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided UDT value.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if value {@code i} is not a UDT value or if its definition
     * does not correspond to the one of {@code v}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a UDT value or if the definition of column {@code name} does not correspond to
     * the one of {@code v}.
     */
    public T setUDTValue(String name, UDTValue v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided tuple value.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a tuple value or if the types of column {@code name} do not correspond to
     * the ones of {@code v}.
     */
    public T setTupleValue(String name, TupleValue v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to {@code null}.
     * <p>
     * This is mainly intended for CQL types which map to native Java types.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @return this object.
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     */
    public T setToNull(String name);
}