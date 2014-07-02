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
     */
    public T setDate(int i, Date v);

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
}