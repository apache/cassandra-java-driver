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
     */
    public T setTimestamp(String name, Date v);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided date (without time).
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
    public T setDate(String name, LocalDate v);

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
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method must perform a runtime inspection of the provided list,
     * in order to guess the best codec to serialize the list elements.
     * The result of such inspection cannot be cached and thus must be performed for each invocation
     * of this method, which may incur in a performance penalty.
     * <p>
     * Furthermore, if two or more codecs are available
     * for the underlying CQL type ({@code list}), <em>which one will be used will depend
     * on the actual object being serialized</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setList(String, List, Class)} or
     * {@link #setList(String, List, TypeToken)}.
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
     * Sets the value for (all occurrences of) variable {@code name} to the provided list,
     * whose elements are of the provided Java class.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the list elements,
     * and thus should be used instead of {@link #setList(String, List)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * @param v the value to set.
     * @param elementsClass the class for the elements of the list.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(String name, List<E> v, Class<E> elementsClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided list,
     * whose elements are of the provided Java type.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the list elements,
     * and thus should be used instead of {@link #setList(String, List)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * @param v the value to set.
     * @param elementsType the type for the elements of the list.
     * @return this object.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setList(String name, List<E> v, TypeToken<E> elementsType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     * <p>
     * Note about performance: this method must perform a runtime inspection of the provided map,
     * in order to guess the best codec to serialize the map entries.
     * The result of such inspection cannot be cached and thus must be performed for each invocation
     * of this method, which may incur in a performance penalty.
     * <p>
     * Furthermore, if two or more codecs are available
     * for the underlying CQL type ({@code map}), <em>which one will be used will depend
     * on the actual object being serialized</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setMap(String, Map, Class, Class)} or
     * {@link #setMap(String, Map, TypeToken, TypeToken)}.
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
     * Sets the value for (all occurrences of) variable {@code name} to the provided map,
     * whose keys and values are of the provided Java classes.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the map entries,
     * and thus should be used instead of {@link #setMap(String, Map)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @param keysClass the class for the keys of the map.
     * @param valuesClass the class for the values of the map.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided map,
     * whose keys and values are of the provided Java types.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the map entries,
     * and thus should be used instead of {@link #setMap(String, Map)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @param keysType the type for the keys of the map.
     * @param valuesType the type for the values of the map.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> T setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     * <p>
     * Note about performance: this method must perform a runtime inspection of the provided map,
     * in order to guess the best codec to serialize the set elements.
     * The result of such inspection cannot be cached and thus must be performed for each invocation
     * of this method, which may incur in a performance penalty.
     * <p>
     * Furthermore, if two or more codecs are available
     * for the underlying CQL type ({@code set}), <em>which one will be used will depend
     * on the actual object being serialized</em>.
     * <p>
     * For these reasons, it is generally preferable to use the more
     * deterministic methods {@link #setSet(String, Set, Class)} or
     * {@link #setSet(String, Set, TypeToken)}.
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
     * Sets the value for (all occurrences of) variable {@code name} to the provided set,
     * whose elements are of the provided Java class.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the set elements,
     * and thus should be used instead of {@link #setSet(String, Set)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * @param v the value to set.
     * @param elementsClass the class for the elements of the set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(String name, Set<E> v, Class<E> elementsClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided set,
     * whose elements are of the provided Java type.
     * <p>
     * Please note that {@code null} values inside collections are not supported by CQL.
     * <p>
     * Note about performance: this method is able to cache codecs used to serialize the set elements,
     * and thus should be used instead of {@link #setSet(String, Set)}
     * whenever possible, because it performs significantly better.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * @param v the value to set.
     * @param elementsType the type for the elements of the set.
     * @return this object.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this object.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <E> T setSet(String name, Set<E> v, TypeToken<E> elementsType);

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

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value of the provided Java class.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and the provided class must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set; may be {@code null}.
     * @param targetClass The Java class to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(String name, V v, Class<V> targetClass);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value of the provided Java type.
     * <p>
     * A suitable {@link TypeCodec} instance for the underlying CQL type and the provided class must
     * have been previously registered with the {@link CodecRegistry} currently in use.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set; may be {@code null}.
     * @param targetType The Java type to convert to; must not be {@code null};
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(String name, V v, TypeToken<V> targetType);

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the provided value,
     * converted using the given {@link TypeCodec}.
     * <p>
     * Note that this method allows to entirely bypass the {@link CodecRegistry} currently in use
     * and forces the driver to use the given codec instead.
     * <p>
     * It is the caller's responsibility to ensure that the given codec {@link TypeCodec#accepts(DataType) accepts}
     * the underlying CQL type; failing to do so may result in {@link InvalidTypeException}s being thrown.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set; may be {@code null}.
     * @param codec The {@link TypeCodec} to use to serialize the value; may not be {@code null}.
     * @return this object.
     * @throws InvalidTypeException if the given codec does not {@link TypeCodec#accepts(DataType) accept} the underlying CQL type.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    <V> T set(String name, V v, TypeCodec<V> codec);


}
