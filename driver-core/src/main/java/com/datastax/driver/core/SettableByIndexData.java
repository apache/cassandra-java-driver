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
     */
    public T setTimestamp(int i, Date v);

    /**
     * Set the {@code i}th value to the provided date (without time).
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public T setDate(int i, LocalDate v);

    /**
     * Set the {@code i}th value to the provided time as a long in nanoseconds since midnight.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
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
     */
    public T setInet(int i, InetAddress v);

    /**
     * Sets the {@code i}th value to the provided list.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note: if two or more codecs are available
     * for the underlying CQL type, <em>the one that will be used will be
     * the first one to be registered.</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setList(int, List, Class)} or
     * {@link #setList(int, List, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(int i, List<E> v);

    /**
     * Sets the {@code i}th value to the provided list, whose elements are of the provided
     * Java class.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the list elements,
     * and thus should be used instead of {@link #setList(int, List)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param elementsClass the class for the elements of the list.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(int i, List<E> v, Class<E> elementsClass);

    /**
     * Sets the {@code i}th value to the provided list, whose elements are of the provided
     * Java type.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the list elements,
     * and thus should be used instead of {@link #setList(int, List)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param elementsType the type for the elements of the list.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(int i, List<E> v, TypeToken<E> elementsType);

    /**
     * Sets the {@code i}th value to the provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     * <p>
     * Note: if two or more codecs are available
     * for the underlying CQL type, <em>the one that will be used will be
     * the first one to be registered.</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setMap(int, Map, Class, Class)} or
     * {@link #setMap(int, Map, TypeToken, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(int i, Map<K, V> v);

    /**
     * Sets the {@code i}th value to the provided map, whose keys and values are of the provided
     * Java classes.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the map entries,
     * and thus should be used instead of {@link #setMap(int, Map)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param keysClass the class for the keys of the map.
     * @param valuesClass the class for the values of the map.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Sets the {@code i}th value to the provided map, whose keys and values are of the provided
     * Java types.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the map entries,
     * and thus should be used instead of {@link #setMap(int, Map)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param keysType the type for the keys of the map.
     * @param valuesType the type for the values of the map.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Sets the {@code i}th value to the provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     * <p>
     * Note: if two or more codecs are available
     * for the underlying CQL type, <em>the one that will be used will be
     * the first one to be registered.</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setSet(int, Set, Class)} or
     * {@link #setSet(int, Set, TypeToken)}.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(int i, Set<E> v);

    /**
     * Sets the {@code i}th value to the provided set, whose elements are of the provided
     * Java class.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the set elements,
     * and thus should be used instead of {@link #setSet(int, Set)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param elementsClass the class for the elements of the set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(int i, Set<E> v, Class<E> elementsClass);

    /**
     * Sets the {@code i}th value to the provided set, whose elements are of the provided
     * Java type.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the set elements,
     * and thus should be used instead of {@link #setSet(int, Set)}
     * whenever possible, because it performs significantly better.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @param elementsType the type for the elements of the set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(int i, Set<E> v, TypeToken<E> elementsType);

    /**
     * Sets the {@code i}th value to the provided UDT value.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
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

    /**
     * Sets the {@code i}th value to the provided value of the provided Java class.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and the provided class must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     *
     * @param i the index of the value to set.
     * @param v the value to set; may be {@code null}.
     * @param targetClass The Java class to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(int i, V v, Class<V> targetClass);

    /**
     * Sets the {@code i}th value to the provided value of the provided Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and the provided class must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     *
     * @param i the index of the value to set.
     * @param v the value to set; may be {@code null}.
     * @param targetType The Java type to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(int i, V v, TypeToken<V> targetType);

    /**
     * Sets the {@code i}th value to the provided value, converted using the given {@link TypeCodec}.
     * <p>
     * Note that this method allows to entirely bypass the {@link CodecRegistry} currently in use
     * and forces the driver to use the given codec instead.
     * <p>
     * It is the caller's responsibility to ensure that the given codec {@link TypeCodec#accepts(DataType) accepts}
     * the underlying CQL type; failing to do so may result in {@link InvalidTypeException}s being thrown.
     *
     * @param i the index of the value to set.
     * @param v the value to set; may be {@code null}.
     * @param codec The {@link TypeCodec} to use to serialize the value; may not be {@code null}.
     * @return this object.
     * @throws InvalidTypeException if the given codec does not {@link TypeCodec#accepts(DataType) accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(int i, V v, TypeCodec<V> codec);

}
