/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
 * A CQL Row returned in a {@link ResultSet}.
 * <p>
 * The values of a CQL Row can be retrieve by either index (index starts at 0)
 * or name. When setting them by name, names follow the case insensitivity
 * rules explained in {@link ColumnDefinitions}.
 */
public interface Row extends GettableData {

    /**
     * Returns the columns contained in this Row.
     *
     * @return the columns contained in this Row.
     */
    public ColumnDefinitions getColumnDefinitions();

    /**
     * Returns whether the {@code i}th value of this row is NULL.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to check.
     * @return whether the {@code i}th value of this row is NULL.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     */
    @Override
    public boolean isNull(int i);

    /**
     * Returns whether the value for column {@code name} in this row is NULL.
     *
     * @param name the name of the column to check.
     * @return whether the value of column {@code name} is NULL.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     */
    @Override
    public boolean isNull(String name);

    /**
     * Returns the {@code i}th value of this row as a boolean.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the boolean value of the {@code i}th column in this row. If the
     * value is NULL, {@code false} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BOOLEAN.
     */
    @Override
    public boolean getBool(int i);

    /**
     * Returns the value of column {@code name} as a boolean.
     *
     * @param name the name of the column to retrieve.
     * @return the boolean value of column {@code name}. If the value is NULL,
     * {@code false} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type BOOLEAN.
     */
    @Override
    public boolean getBool(String name);

    /**
     * Returns the {@code i}th value of this row as an integer.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an integer. If the
     * value is NULL, {@code 0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INT.
     */
    @Override
    public int getInt(int i);

    /**
     * Returns the value of column {@code name} as an integer.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as an integer. If the value is NULL,
     * {@code 0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type INT.
     */
    @Override
    public int getInt(String name);

    /**
     * Returns the {@code i}th value of this row as a long.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a long. If the
     * value is NULL, {@code 0L} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BIGINT or COUNTER.
     */
    @Override
    public long getLong(int i);

    /**
     * Returns the value of column {@code name} as a long.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a long. If the value is NULL,
     * {@code 0L} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not of type BIGINT or COUNTER.
     */
    @Override
    public long getLong(String name);

    /**
     * Returns the {@code i}th value of this row as a date.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a data. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type TIMESTAMP.
     */
    @Override
    public Date getDate(int i);

    /**
     * Returns the value of column {@code name} as a date.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a date. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type TIMESTAMP.
     */
    @Override
    public Date getDate(String name);

    /**
     * Returns the {@code i}th value of this row as a float.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a float. If the
     * value is NULL, {@code 0.0f} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type FLOAT.
     */
    @Override
    public float getFloat(int i);

    /**
     * Returns the value of column {@code name} as a float.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a float. If the value is NULL,
     * {@code 0.0f} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type FLOAT.
     */
    @Override
    public float getFloat(String name);

    /**
     * Returns the {@code i}th value of this row as a double.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a double. If the
     * value is NULL, {@code 0.0} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DOUBLE.
     */
    @Override
    public double getDouble(int i);

    /**
     * Returns the value of column {@code name} as a double.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a double. If the value is NULL,
     * {@code 0.0} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type DOUBLE.
     */
    @Override
    public double getDouble(String name);

    /**
     * Returns the {@code i}th value of this row as a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a ByteBuffer. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i);

    /**
     * Returns the value of column {@code name} as a ByteBuffer.
     *
     * Note: this method always return the bytes composing the value, even if
     * the column is not of type BLOB. That is, this method never throw an
     * InvalidTypeException. However, if the type is not BLOB, it is up to the
     * caller to handle the returned value correctly.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a ByteBuffer. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     */
    @Override
    public ByteBuffer getBytesUnsafe(String name);

    /**
     * Returns the {@code i}th value of this row as a byte array.
     * <p>
     * Note that this method validate that the column is of type BLOB. If you want to retrieve
     * the bytes for any type of columns, use {@link #getBytesUnsafe(int)} instead.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a byte array. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} type is not of type BLOB.
     */
    @Override
    public ByteBuffer getBytes(int i);

    /**
     * Returns the value of column {@code name} as a byte array.
     * <p>
     * Note that this method validate that the column is of type BLOB. If you want to retrieve
     * the bytes for any type of columns, use {@link #getBytesUnsafe(String)} instead.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a byte array. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} type is not of type BLOB.
     */
    @Override
    public ByteBuffer getBytes(String name);

    /**
     * Returns the {@code i}th value of this row as a string.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a string. If the
     * value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    @Override
    public String getString(int i);

    /**
     * Returns the value of column {@code name} as a string.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a string. If the value is NULL,
     * {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} type is none of:
     * VARCHAR, TEXT or ASCII.
     */
    @Override
    public String getString(String name);

    /**
     * Returns the {@code i}th value of this row as a variable length integer.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a variable
     * length integer. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type VARINT.
     */
    @Override
    public BigInteger getVarint(int i);

    /**
     * Returns the value of column {@code name} as a variable length integer.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a variable length integer.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type VARINT.
     */
    @Override
    public BigInteger getVarint(String name);

    /**
     * Returns the {@code i}th value of this row as a variable length decimal.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a variable
     * length decimal. If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DECIMAL.
     */
    @Override
    public BigDecimal getDecimal(int i);

    /**
     * Returns the value of column {@code name} as a variable length decimal.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a variable length decimal.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type DECIMAL.
     */
    @Override
    public BigDecimal getDecimal(String name);

    /**
     * Returns the {@code i}th value of this row as a UUID.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type UUID
     * or TIMEUUID.
     */
    @Override
    public UUID getUUID(int i);

    /**
     * Returns the value of column {@code name} as a UUID.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a UUID.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type
     * UUID or TIMEUUID.
     */
    @Override
    public UUID getUUID(String name);

    /**
     * Returns the {@code i}th value of this row as an InetAddress.
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INET.
     */
    @Override
    public InetAddress getInet(int i);

    /**
     * Returns the value of column {@code name} as an InetAddress.
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as an InetAddress.
     * If the value is NULL, {@code null} is returned.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type
     * INET.
     */
    @Override
    public InetAddress getInet(String name);

    /**
     * Returns the {@code i}th value of this row as a {@link Token}.
     * <p>
     * {@link #getPartitionKeyToken()} should generally be preferred to this method (unless the
     * token column is aliased).
     *
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @return the value of the {@code i}th column in this row as an Token.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not of the type of token values
     * for this cluster (this depends on the configured partitioner).
     */
    public Token getToken(int i);

    /**
     * Returns the value of column {@code name} as a {@link Token}.
     * <p>
     * {@link #getPartitionKeyToken()} should generally be preferred to this method (unless the
     * token column is aliased).
     *
     * @param name the name of the column to retrieve.
     * @return the value of column {@code name} as a Token.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of the type of token values
     * for this cluster (this depends on the configured partitioner).
     */
    public Token getToken(String name);

    /**
     * Returns the value of the first column containing a {@link Token}.
     * <p>
     * This method is a shorthand for queries returning a single token in an unaliased
     * column. It will look for the first name matching {@code token(...)}:
     * <pre>
     * {@code
     * ResultSet rs = session.execute("SELECT token(k) FROM my_table WHERE k = 1");
     * Token token = rs.one().getPartitionKeyToken(); // retrieves token(k)
     * }
     * </pre>
     * If that doesn't work for you (for example, if you're using an alias), use
     * {@link #getToken(int)} or {@link #getToken(String)}.
     *
     * @return the value of column {@code name} as a Token.
     *
     * @throws IllegalStateException if no column named {@code token(...)} exists in this
     * ResultSet.
     * @throws InvalidTypeException if the first column named {@code token(...)} is not of
     * the type of token values for this cluster (this depends on the configured partitioner).
     */
    public Token getPartitionKeyToken();

    /**
     * Returns the {@code i}th value of this row as a list.
     *
     * @param <T> the type of the elements of the list to return.
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th column in this row as a list of
     * {@code elementsClass} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass);

    /**
     * Returns the value of column {@code name} as a list.
     *
     * @param <T> the type of the elements of the list to return.
     * @param name the name of the column to retrieve.
     * @param elementsClass the class for the elements of the list to retrieve.
     * @return the value of the {@code i}th column in this row as a list of
     * {@code elementsClass} objects. If the value is NULL, an empty list is
     * returned (note that Cassandra makes no difference between an empty list
     * and column of type list that is not set). The returned list is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a list or if its
     * elements are not of class {@code elementsClass}.
     */
    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value of this row as a set.
     *
     * @param <T> the type of the elements of the set to return.
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th column in this row as a set of
     * {@code elementsClass} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass);

    /**
     * Returns the value of column {@code name} as a set.
     *
     * @param <T> the type of the elements of the set to return.
     * @param name the name of the column to retrieve.
     * @param elementsClass the class for the elements of the set to retrieve.
     * @return the value of the {@code i}th column in this row as a set of
     * {@code elementsClass} objects. If the value is NULL, an empty set is
     * returned (note that Cassandra makes no difference between an empty set
     * and column of type set that is not set). The returned set is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a set or if its
     * elements are not of class {@code elementsClass}.
     */
    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass);

    /**
     * Returns the {@code i}th value of this row as a map.
     *
     * @param <K> the type of the keys of the map to return.
     * @param <V> the type of the values of the map to return.
     * @param i the index ({@code 0 <= i < size()}) of the column to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th column in this row as a map of
     * {@code keysClass} to {@code valuesClass} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.columns().size()}.
     * @throws InvalidTypeException if column {@code i} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass);

    /**
     * Returns the value of column {@code name} as a map.
     *
     * @param <K> the type of the keys of the map to return.
     * @param <V> the type of the values of the map to return.
     * @param name the name of the column to retrieve.
     * @param keysClass the class for the keys of the map to retrieve.
     * @param valuesClass the class for the values of the map to retrieve.
     * @return the value of the {@code i}th column in this row as a map of
     * {@code keysClass} to {@code valuesClass} objects. If the value is NULL,
     * an empty map is returned (note that Cassandra makes no difference
     * between an empty map and column of type map that is not set). The
     * returned map is immutable.
     *
     * @throws IllegalArgumentException if {@code name} is not part of the
     * ResultSet this row is part of, i.e. if {@code !this.columns().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a map, if its
     * keys are not of class {@code keysClass} or if its values are not of
     * class {@code valuesClass}.
     */
    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass);
}
