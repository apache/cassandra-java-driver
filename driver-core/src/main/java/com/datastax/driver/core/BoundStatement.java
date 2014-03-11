/*
 *      Copyright (C) 2012 DataStax Inc.
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

import org.apache.cassandra.db.marshal.*;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A prepared statement with values bound to the bind variables.
 * <p>
 * Once values has been provided for the variables of the {@link PreparedStatement}
 * it has been created from, such BoundStatement can be executed (through 
 * {@link Session#execute(Query)}).
 * <p>
 * The values of a BoundStatement can be set by either index or name. When
 * setting them by name, names follow the case insensitivity rules explained in
 * {@link ColumnDefinitions}. If multiple bind variables
 * correspond to the same column (as would be the case if you prepare
 * {@code SELECT * FROM t WHERE x > ? AND x < ?}), you will have to set
 * values by indexes (or the {@link #bind} method) as the methods to set by
 * name only allows to set the first prepared occurrence of the column.
 * <p>
 * Any variable that hasn't been specifically set will be considered {@code null}.
 */
public class BoundStatement extends Query {

    final PreparedStatement statement;
    final ByteBuffer[] values;

    /**
     * Creates a new {@code BoundStatement} from the provided prepared
     * statement.
     * @param statement the prepared statement from which to create a {@code BoundStatement}.
     */
    public BoundStatement(PreparedStatement statement) {
        this.statement = statement;
        this.values = new ByteBuffer[statement.getVariables().size()];

        if (statement.getConsistencyLevel() != null)
            this.setConsistencyLevel(statement.getConsistencyLevel());
        if (statement.isTracing())
            this.enableTracing();
        if (statement.getRetryPolicy() != null)
            this.setRetryPolicy(statement.getRetryPolicy());
    }

    /**
     * Returns the prepared statement on which this BoundStatement is based.
     *
     * @return the prepared statement on which this BoundStatement is based.
     */
    public PreparedStatement preparedStatement() {
        return statement;
    }

    /**
     * Returns whether the {@code i}th variable has been bound to a non null value.
     *
     * @param i the index of the variable to check.
     * @return whether the {@code i}th variable has been bound to a non null value.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     */
    public boolean isSet(int i) {
        metadata().checkBounds(i);
        return values[i] != null;
    }

    /**
     * Returns whether the first occurrence of variable {@code name} has been
     * bound to a non-null value.
     *
     * @param name the name of the variable to check.
     * @return whether the first occurrence of variable {@code name} has been 
     * bound to a non-null value.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public boolean isSet(String name) {
        return isSet(metadata().getIdx(name));
    }

    /**
     * Bound values to the variables of this statement.
     *
     * This is a convenience method to bind all the variables of the
     * {@code BoundStatement} in one call.
     *
     * @param values the values to bind to the variables of the newly created
     * BoundStatement. The first element of {@code values} will be bound to the
     * first bind variable, etc. It is legal to provide fewer values than the
     * statement has bound variables. In that case, the remaining variable need
     * to be bound before execution. If more values than variables are provided
     * however, an IllegalArgumentException wil be raised.
     * @return this bound statement.
     *
     * @throws IllegalArgumentException if more {@code values} are provided
     * than there is of bound variables in this statement.
     * @throws InvalidTypeException if any of the provided value is not of
     * correct type to be bound to the corresponding bind variable.
     * @throws NullPointerException if one of {@code values} is a collection
     * (List, Set or Map) containing a null value. Nulls are not supported in
     * collections by CQL.
     */
    public BoundStatement bind(Object... values) {

        if (values.length > statement.getVariables().size())
            throw new IllegalArgumentException(String.format("Prepared statement has only %d variables, %d values provided", statement.getVariables().size(), values.length));

        for (int i = 0; i < values.length; i++)
        {
            Object toSet = values[i];

            if (toSet == null) {
                setValue(i, null);
                continue;
            }

            DataType columnType = statement.getVariables().getType(i);
            switch (columnType.getName()) {
                case LIST:
                    if (!(toSet instanceof List))
                        throw new InvalidTypeException(String.format("Invalid type for value %d, column is a list but %s provided", i, toSet.getClass()));

                    List<?> l = (List<?>)toSet;
                    // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
                    if (!l.isEmpty()) {
                        // Ugly? Yes
                        Class<?> providedClass = l.get(0).getClass();
                        Class<?> expectedClass = columnType.getTypeArguments().get(0).asJavaClass();
                        if (!expectedClass.isAssignableFrom(providedClass))
                            throw new InvalidTypeException(String.format("Invalid type for value %d of CQL type %s, expecting list of %s but provided list of %s", i, columnType, expectedClass, providedClass));
                    }
                    break;
                case SET:
                    if (!(toSet instanceof Set))
                        throw new InvalidTypeException(String.format("Invalid type for value %d, column is a set but %s provided", i, toSet.getClass()));

                    Set<?> s = (Set<?>)toSet;
                    // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
                    if (!s.isEmpty()) {
                        // Ugly? Yes
                        Class<?> providedClass = s.iterator().next().getClass();
                        Class<?> expectedClass = columnType.getTypeArguments().get(0).getName().javaType;
                        if (!expectedClass.isAssignableFrom(providedClass))
                            throw new InvalidTypeException(String.format("Invalid type for value %d of CQL type %s, expecting set of %s but provided set of %s", i, columnType, expectedClass, providedClass));
                    }
                    break;
                case MAP:
                    if (!(toSet instanceof Map))
                        throw new InvalidTypeException(String.format("Invalid type for value %d, column is a map but %s provided", i, toSet.getClass()));

                    Map<?, ?> m = (Map<?, ?>)toSet;
                    // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
                    if (!m.isEmpty()) {
                        // Ugly? Yes
                        Map.Entry<?, ?> entry = m.entrySet().iterator().next();
                        Class<?> providedKeysClass = entry.getKey().getClass();
                        Class<?> providedValuesClass = entry.getValue().getClass();

                        Class<?> expectedKeysClass = columnType.getTypeArguments().get(0).getName().javaType;
                        Class<?> expectedValuesClass = columnType.getTypeArguments().get(1).getName().javaType;
                        if (!expectedKeysClass.isAssignableFrom(providedKeysClass) || !expectedValuesClass.isAssignableFrom(providedValuesClass))
                            throw new InvalidTypeException(String.format("Invalid type for value %d of CQL type %s, expecting map of %s->%s but provided set of %s->%s", i, columnType, expectedKeysClass, expectedValuesClass, providedKeysClass, providedValuesClass));
                    }
                    break;
                default:
                    Class<?> providedClass = toSet.getClass();
                    Class<?> expectedClass = columnType.getName().javaType;
                    if (!expectedClass.isAssignableFrom(providedClass))
                        throw new InvalidTypeException(String.format("Invalid type for value %d of CQL type %s, expecting %s but %s provided", i, columnType, expectedClass, providedClass));
                    break;
            }
            setValue(i, Codec.getCodec(columnType).decompose(toSet));
        }
        return this;
    }

    /**
     * The routing key for this bound query.
     * <p>
     * This method will return a non-{@code null} value if either of the following occur:
     * <ul>
     *   <li>All the columns composing the partition key are bound
     *   variables of this {@code BoundStatement}. The routing key will then be
     *   built using the values provided for these partition key columns.</li>
     *   <li>The routing key has been set through {@link PreparedStatement#setRoutingKey}
     *   for the {@code PreparedStatement} this statement has been built from.</li>
     * </ul>
     * Otherwise, {@code null} is returned.
     * <p>
     * Note that if the routing key has been set through {@link PreparedStatement#setRoutingKey},
     * that value takes precedence even if the partition key is part of the bound variables.
     *
     * @return the routing key for this statement or {@code null}.
     */
    @Override
    public ByteBuffer getRoutingKey() {
        if (statement.routingKey != null)
            return statement.routingKey;

        if (statement.routingKeyIndexes != null) {
            if (statement.routingKeyIndexes.length == 1) {
                return values[statement.routingKeyIndexes[0]];
            } else {
                ByteBuffer[] components = new ByteBuffer[statement.routingKeyIndexes.length];
                for (int i = 0; i < components.length; ++i) {
                    ByteBuffer value = values[statement.routingKeyIndexes[i]];
                    if (value == null)
                        return null;
                    components[i] = value;
                }
                return SimpleStatement.compose(components);
            }
        }
        return null;
    }

    /**
     * Set the {@code i}th value to the provided boolean.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BOOLEAN.
     */
    public BoundStatement setBool(int i, boolean v) {
        metadata().checkType(i, DataType.Name.BOOLEAN);
        return setValue(i, BooleanType.instance.decompose(v));
    }

    /**
     * Set the value for the first occurrence of column {@code name} to the provided boolean.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type BOOLEAN.
     */
    public BoundStatement setBool(String name, boolean v) {
        return setBool(metadata().getIdx(name), v);
    }

    /**
     * Set the {@code i}th value to the provided integer.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INT.
     */
    public BoundStatement setInt(int i, int v) {
        metadata().checkType(i, DataType.Name.INT);
        return setValue(i, Int32Type.instance.decompose(v));
    }

    /**
     * Set the value for the first occurrence of column {@code name} to the provided integer.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not of type INT.
     */
    public BoundStatement setInt(String name, int v) {
        return setInt(metadata().getIdx(name), v);
    }

    /**
     * Set the {@code i}th value to the provided long.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is of type BIGINT or COUNTER.
     */
    public BoundStatement setLong(int i, long v) {
        metadata().checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);
        return setValue(i, LongType.instance.decompose(v));
    }

    /**
     * Set the value for the first occurrence of column {@code name} to the provided long.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is of type BIGINT or COUNTER.
     */
    public BoundStatement setLong(String name, long v) {
        return setLong(metadata().getIdx(name), v);
    }

    /**
     * Set the {@code i}th value to the provided date.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type TIMESTAMP.
     */
    public BoundStatement setDate(int i, Date v) {
        metadata().checkType(i, DataType.Name.TIMESTAMP);
        return setValue(i, v == null ? null : DateType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided date.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type TIMESTAMP.
     */
    public BoundStatement setDate(String name, Date v) {
        return setDate(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided float.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type FLOAT.
     */
    public BoundStatement setFloat(int i, float v) {
        metadata().checkType(i, DataType.Name.FLOAT);
        return setValue(i, FloatType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided float.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not of type FLOAT.
     */
    public BoundStatement setFloat(String name, float v) {
        return setFloat(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided double.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DOUBLE.
     */
    public BoundStatement setDouble(int i, double v) {
        metadata().checkType(i, DataType.Name.DOUBLE);
        return setValue(i, DoubleType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided double.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code i} is not of type DOUBLE.
     */
    public BoundStatement setDouble(String name, double v) {
        return setDouble(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided string.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is of neither of the
     * following types: VARCHAR, TEXT or ASCII.
     */
    public BoundStatement setString(int i, String v) {
        DataType.Name type = metadata().checkType(i, DataType.Name.VARCHAR,
                                                     DataType.Name.TEXT,
                                                     DataType.Name.ASCII);
        switch (type) {
            case ASCII:
                return setValue(i, v == null ? null : AsciiType.instance.decompose(v));
            case TEXT:
            case VARCHAR:
                return setValue(i, v == null ? null : UTF8Type.instance.decompose(v));
            default:
                throw new AssertionError();
        }
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided string.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is of neither of the
     * following types: VARCHAR, TEXT or ASCII.
     */
    public BoundStatement setString(String name, String v) {
        return setString(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * This method validate that the type of the column set is BLOB. If you
     * want to insert manually serialized data into columns of another type,
     * use {@link #setBytesUnsafe} instead.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BLOB.
     */
    public BoundStatement setBytes(int i, ByteBuffer v) {
        metadata().checkType(i, DataType.Name.BLOB);
        return setBytesUnsafe(i, v);
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided byte buffer.
     *
     * This method validate that the type of the column set is BLOB. If you
     * want to insert manually serialized data into columns of another type,
     * use {@link #setBytesUnsafe} instead.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type BLOB.
     */
    public BoundStatement setBytes(String name, ByteBuffer v) {
        return setBytes(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided byte buffer.
     *
     * Contrary to {@link #setBytes}, this method does not check the
     * type of the column set. If you insert data that is not compatible with
     * the type of the column, you will get an {@code InvalidQueryException} at
     * execute time.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     */
    public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided byte buffer.
     *
     * Contrary to {@link #setBytes}, this method does not check the
     * type of the column set. If you insert data that is not compatible with
     * the type of the column, you will get an {@code InvalidQueryException} at
     * execute time.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public BoundStatement setBytesUnsafe(String name, ByteBuffer v) {
        return setBytesUnsafe(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided big integer.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type VARINT.
     */
    public BoundStatement setVarint(int i, BigInteger v) {
        metadata().checkType(i, DataType.Name.VARINT);
        return setValue(i, v == null ? null : IntegerType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided big integer.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type VARINT.
     */
    public BoundStatement setVarint(String name, BigInteger v) {
        return setVarint(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided big decimal.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type DECIMAL.
     */
    public BoundStatement setDecimal(int i, BigDecimal v) {
        metadata().checkType(i, DataType.Name.DECIMAL);
        return setValue(i, v == null ? null : DecimalType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided big decimal.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type DECIMAL.
     */
    public BoundStatement setDecimal(String name, BigDecimal v) {
        return setDecimal(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided UUID.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type UUID or
     * TIMEUUID, or if column {@code i} is of type TIMEUUID but {@code v} is
     * not a type 1 UUID.
     */
    public BoundStatement setUUID(int i, UUID v) {
        DataType.Name type = metadata().checkType(i, DataType.Name.UUID,
                                                       DataType.Name.TIMEUUID);

        if (v == null)
            return setValue(i, null);

        if (type == DataType.Name.TIMEUUID && v.version() != 1)
            throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));

        return type == DataType.Name.UUID
             ? setValue(i, UUIDType.instance.decompose(v))
             : setValue(i, TimeUUIDType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided UUID.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type UUID or
     * TIMEUUID, or if column {@code name} is of type TIMEUUID but {@code v} is
     * not a type 1 UUID.
     */
    public BoundStatement setUUID(String name, UUID v) {
        return setUUID(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided inet address.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type INET.
     */
    public BoundStatement setInet(int i, InetAddress v) {
        metadata().checkType(i, DataType.Name.INET);
        return setValue(i, v == null ? null : InetAddressType.instance.decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided inet address.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not of type INET.
     */
    public BoundStatement setInet(String name, InetAddress v) {
        return setInet(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not a list type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setList(int i, List<T> v) {
        DataType type = metadata().getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a list", metadata().getName(i), type));

        if (v == null)
            return setValue(i, null);

        // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.get(0).getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).asJavaClass();

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting list of %s but provided list of %s", metadata().getName(i), type, expectedClass, providedClass));
        }

        return setValue(i, Codec.<List<T>>getCodec(type).decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a list type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setList(String name, List<T> v) {
        return setList(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not a map type or
     * if the elements (keys or values) of {@code v} are not of the type of the
     * elements of column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> BoundStatement setMap(int i, Map<K, V> v) {
        DataType type = metadata().getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a map", metadata().getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            Class<?> providedKeysClass = entry.getKey().getClass();
            Class<?> providedValuesClass = entry.getValue().getClass();

            Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
            Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
            if (!expectedKeysClass.isAssignableFrom(providedKeysClass) || !expectedValuesClass.isAssignableFrom(providedValuesClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting map of %s->%s but provided map of %s->%s", metadata().getName(i), type, expectedKeysClass, expectedValuesClass, providedKeysClass, providedValuesClass));
        }

        return setValue(i, Codec.<Map<K, V>>getCodec(type).decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a map type or
     * if the elements (keys or values) of {@code v} are not of the type of the
     * elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> BoundStatement setMap(String name, Map<K, V> v) {
        return setMap(metadata().getIdx(name), v);
    }

    /**
     * Sets the {@code i}th value to the provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not a set type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code i}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setSet(int i, Set<T> v) {
        DataType type = metadata().getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a set", metadata().getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.iterator().next().getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting set of %s but provided set of %s", metadata().getName(i), type, expectedClass, providedClass));
        }

        return setValue(i, Codec.<Set<T>>getCodec(type).decompose(v));
    }

    /**
     * Sets the value for the first occurrence of column {@code name} to the provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, only the first one is set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if column {@code name} is not a set type or
     * if the elements of {@code v} are not of the type of the elements of
     * column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setSet(String name, Set<T> v) {
        return setSet(metadata().getIdx(name), v);
    }

    private ColumnDefinitions metadata() {
        return statement.metadata;
    }

    private BoundStatement setValue(int i, ByteBuffer value) {
        values[i] = value;
        return this;
    }
}
