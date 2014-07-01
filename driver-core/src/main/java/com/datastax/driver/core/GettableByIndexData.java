package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

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
    public Date getDate(int i);

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
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th element as a list of
     * {@code elementsClass} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> List<T> getList(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a set.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th element as a set of
     * {@code elementsClass} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value as a map.
     *
     * @param i the index ({@code 0 <= i < size()}) to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th element as a map of
     * {@code keysClass} to {@code valuesClass} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     * @throws InvalidTypeException if value {@code i} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass);

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
}