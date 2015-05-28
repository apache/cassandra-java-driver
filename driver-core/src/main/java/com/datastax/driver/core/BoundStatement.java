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
 * A prepared statement with values bound to the bind variables.
 * <p>
 * Once values has been provided for the variables of the {@link PreparedStatement}
 * it has been created from, such BoundStatement can be executed (through
 * {@link Session#execute(Statement)}).
 * <p>
 * The values of a BoundStatement can be set by either index or name. When
 * setting them by name, names follow the case insensitivity rules explained in
 * {@link ColumnDefinitions} but with the difference that if multiple bind
 * variables have the same name, setting that name will set <b>all</b> the
 * variables for that name.
 * <p>
 * All the variables of the statement must be bound. If you don't explicitly
 * set a value for a variable, an {@code IllegalStateException} will be
 * thrown when submitting the statement. If you want to set a variable to
 * {@code null}, use {@link #setToNull(int) setToNull}.
 */
public class BoundStatement extends Statement implements SettableData<BoundStatement>, GettableData {
    static final ByteBuffer UNSET = ByteBuffer.allocate(0);

    final PreparedStatement statement;

    // Statement is already an abstract class, so we can't make it extend AbstractData directly. But
    // we still want to avoid duplicating too much code so we wrap.
    final DataWrapper wrapper;

    private ByteBuffer routingKey;

    /**
     * Creates a new {@code BoundStatement} from the provided prepared
     * statement.
     * @param statement the prepared statement from which to create a {@code BoundStatement}.
     */
    public BoundStatement(PreparedStatement statement) {
        this.statement = statement;
        this.wrapper = new DataWrapper(this, statement.getVariables().size());
        for (int i = 0; i < wrapper.values.length; i++) {
            wrapper.values[i] = UNSET;
        }

        if (statement.getConsistencyLevel() != null)
            this.setConsistencyLevel(statement.getConsistencyLevel());
        if (statement.getSerialConsistencyLevel() != null)
            this.setSerialConsistencyLevel(statement.getSerialConsistencyLevel());
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
     * Returns whether the {@code i}th variable has been bound.
     *
     * @param i the index of the variable to check.
     * @return whether the {@code i}th variable has been bound.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     */
    public boolean isSet(int i) {
        return wrapper.getValue(i) != UNSET;
    }

    /**
     * Returns whether the first occurrence of variable {@code name} has been
     * bound.
     *
     * @param name the name of the variable to check.
     * @return whether the first occurrence of variable {@code name} has been
     * bound to a non-null value.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public boolean isSet(String name) {
        return wrapper.getValue(wrapper.getIndexOf(name)) != UNSET;
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
                wrapper.values[i] = null;
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
                    if (toSet instanceof Token)
                        toSet = ((Token)toSet).getValue();

                    Class<?> providedClass = toSet.getClass();
                    Class<?> expectedClass = columnType.getName().javaType;
                    if (!expectedClass.isAssignableFrom(providedClass))
                        throw new InvalidTypeException(String.format("Invalid type for value %d of CQL type %s, expecting %s but %s provided", i, columnType, expectedClass, providedClass));
                    break;
            }
            wrapper.values[i] = columnType.codec(statement.getPreparedId().protocolVersion).serialize(toSet);
        }
        return this;
    }

    /**
     * Sets the routing key for this bound statement.
     * <p>
     * This is useful when the routing key can neither be set on the {@code PreparedStatement} this bound statement
     * was built from, nor automatically computed from bound variables. In particular, this is the case if the
     * partition key is composite and only some of its components are bound.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code BoundStatement} object.
     *
     * @see BoundStatement#getRoutingKey
     */
    public BoundStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * The routing key for this bound query.
     * <p>
     * This method will return a non-{@code null} value if either of the following occur:
     * <ul>
     * <li>The routing key has been set directly through {@link BoundStatement#setRoutingKey}.</li>
     * <li>The routing key has been set through {@link PreparedStatement#setRoutingKey} for the
     * {@code PreparedStatement} this statement has been built from.</li>
     * <li>All the columns composing the partition key are bound variables of this {@code BoundStatement}. The routing
     * key will then be built using the values provided for these partition key columns.</li>
     * </ul>
     * Otherwise, {@code null} is returned.
     * <p>
     *
     * Note that if the routing key has been set through {@link BoundStatement#setRoutingKey}, then that takes
     * precedence. If the routing key has been set through {@link PreparedStatement#setRoutingKey} then that is used
     * next. If neither of those are set then it is computed.
     *
     * @return the routing key for this statement or {@code null}.
     */
    @Override
    public ByteBuffer getRoutingKey() {
        if (this.routingKey != null) {
            return this.routingKey;
        }

        if (statement.getRoutingKey() != null) {
            return statement.getRoutingKey();
        }

        int[] rkIndexes = statement.getPreparedId().routingKeyIndexes;
        if (rkIndexes != null) {
            if (rkIndexes.length == 1) {
                return wrapper.values[rkIndexes[0]];
            } else {
                ByteBuffer[] components = new ByteBuffer[rkIndexes.length];
                for (int i = 0; i < components.length; ++i) {
                    ByteBuffer value = wrapper.values[rkIndexes[i]];
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
     * Returns the keyspace this query operates on.
     * <p>
     * This method will always return a non-{@code null} value (unless the statement
     * has no variables, but you should avoid prepared statement in the first in that
     * case). The keyspace returned will be the one corresponding to the first
     * variable prepared in this statement (which in almost all case will be <i>the</i>
     * keyspace for the operation, though it's possible in CQL to build a batch
     * statement that acts on multiple keyspace).
     *
     * @return the keyspace for this statement (see above), or {@code null} if the
     * statement has no variables.
     */
    @Override
    public String getKeyspace() {
        return statement.getPreparedId().metadata.size() == 0 ? null : statement.getPreparedId().metadata.getKeyspace(0);
    }

    /**
     * Sets the {@code i}th value to the provided boolean.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BOOLEAN.
     */
    public BoundStatement setBool(int i, boolean v) {
        return wrapper.setBool(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided boolean.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type BOOLEAN.
     */
    public BoundStatement setBool(String name, boolean v) {
        return wrapper.setBool(name, v);
    }

    /**
     * Set the {@code i}th value to the provided byte.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type TINYINT.
     */
    public BoundStatement setByte(int i, byte v) {
        return wrapper.setByte(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type TINYINT.
     */
    public BoundStatement setByte(String name, byte v) {
        return wrapper.setByte(name, v);
    }

    /**
     * Set the {@code i}th value to the provided short.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type SMALLINT.
     */
    public BoundStatement setShort(int i, short v) {
        return wrapper.setShort(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided short.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type SMALLINT.
     */
    public BoundStatement setShort(String name, short v) {
        return wrapper.setShort(name, v);
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
        return wrapper.setInt(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided integer.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any one occurrence of) {@code name} is not of type INT.
     */
    public BoundStatement setInt(String name, int v) {
        return wrapper.setInt(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided long.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of type BIGINT or COUNTER.
     */
    public BoundStatement setLong(int i, long v) {
        return wrapper.setLong(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided long.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type BIGINT or COUNTER.
     */
    public BoundStatement setLong(String name, long v) {
        return wrapper.setLong(name, v);
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
        return wrapper.setDate(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided date.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type TIMESTAMP.
     */
    public BoundStatement setDate(String name, Date v) {
        return wrapper.setDate(name, v);
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
        return wrapper.setFloat(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided float.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type FLOAT.
     */
    public BoundStatement setFloat(String name, float v) {
        return wrapper.setFloat(name, v);
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
        return wrapper.setDouble(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided double.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type DOUBLE.
     */
    public BoundStatement setDouble(String name, double v) {
        return wrapper.setDouble(name, v);
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
        return wrapper.setString(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided string.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * of neither of the following types: VARCHAR, TEXT or ASCII.
     */
    public BoundStatement setString(String name, String v) {
        return wrapper.setString(name, v);
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
        return wrapper.setBytes(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte buffer.
     *
     * This method validate that the type of the column set is BLOB. If you
     * want to insert manually serialized data into columns of another type,
     * use {@link #setBytesUnsafe} instead.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is not of type BLOB.
     */
    public BoundStatement setBytes(String name, ByteBuffer v) {
        return wrapper.setBytes(name, v);
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
        return wrapper.setBytesUnsafe(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided byte buffer.
     *
     * Contrary to {@link #setBytes}, this method does not check the
     * type of the column set. If you insert data that is not compatible with
     * the type of the column, you will get an {@code InvalidQueryException} at
     * execute time.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public BoundStatement setBytesUnsafe(String name, ByteBuffer v) {
        return wrapper.setBytesUnsafe(name, v);
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
        return wrapper.setVarint(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided big integer.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type VARINT.
     */
    public BoundStatement setVarint(String name, BigInteger v) {
        return wrapper.setVarint(name, v);
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
        return wrapper.setDecimal(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided big decimal.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type DECIMAL.
     */
    public BoundStatement setDecimal(String name, BigDecimal v) {
        return wrapper.setDecimal(name, v);
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
        return wrapper.setUUID(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided UUID.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type UUID or TIMEUUID, or if column {@code name} is of type
     * TIMEUUID but {@code v} is not a type 1 UUID.
     */
    public BoundStatement setUUID(String name, UUID v) {
        return wrapper.setUUID(name, v);
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
        return wrapper.setInet(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided inet address.
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of type INET.
     */
    public BoundStatement setInet(String name, InetAddress v) {
        return wrapper.setInet(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided {@link Token}.
     * <p>
     * {@link #setPartitionKeyToken(Token)} should generally be preferred if you
     * have a single token variable.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException if column {@code i} is not of the type of the token's value.
     */
    public BoundStatement setToken(int i, Token v) {
        return wrapper.setToken(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided token.
     * <p>
     * {@link #setPartitionKeyToken(Token)} should generally be preferred if you
     * have a single token variable.
     * <p>
     * If you have multiple token variables, use positional binding ({@link #setToken(int, Token)},
     * or named bind markers:
     * <pre>
     * {@code
     * PreparedStatement pst = session.prepare("SELECT * FROM my_table WHERE token(k) > :min AND token(k) <= :max");
     * BoundStatement b = pst.bind().setToken("min", minToken).setToken("max", maxToken);
     * }
     * </pre>
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of the type of the token's value.
     */
    public BoundStatement setToken(String name, Token v) {
        return wrapper.setToken(name, v);
    }

    /**
     * Sets the value for (all occurrences of) variable "{@code partition key token}"
     * to the provided token (this is the name generated by Cassandra for markers
     * corresponding to a {@code token(...)} call).
     * <p>
     * This method is a shorthand for statements with a single token variable:
     * <pre>
     * {@code
     * Token token = ...
     * PreparedStatement pst = session.prepare("SELECT * FROM my_table WHERE token(k) = ?");
     * BoundStatement b = pst.bind().setPartitionKeyToken(token);
     * }
     * </pre>
     * If you have multiple token variables, use positional binding ({@link #setToken(int, Token)},
     * or named bind markers:
     * <pre>
     * {@code
     * PreparedStatement pst = session.prepare("SELECT * FROM my_table WHERE token(k) > :min AND token(k) <= :max");
     * BoundStatement b = pst.bind().setToken("min", minToken).setToken("max", maxToken);
     * }
     * </pre>
     *
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not of the type of the token's value.
     */
    public BoundStatement setPartitionKeyToken(Token v) {
        return setToken("partition key token", v);
    }

    /**
     * Sets the {@code i}th value to the provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <T> the type of the elements of the list to set.
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
        return wrapper.setList(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided list.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <T> the type of the elements of the list to set.
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a list type or if the elements of {@code v} are not of the type of
     * the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setList(String name, List<T> v) {
        return wrapper.setList(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <K> the type of the keys for the map to set.
     * @param <V> the type of the values for the map to set.
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
        return wrapper.setMap(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided map.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <K> the type of the keys for the map to set.
     * @param <V> the type of the values for the map to set.
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements (keys or values) of {@code v} are not of
     * the type of the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <K, V> BoundStatement setMap(String name, Map<K, V> v) {
        return wrapper.setMap(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <T> the type of the elements of the set to set.
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
        return wrapper.setSet(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided set.
     * <p>
     * Please note that {@code null} values are not supported inside collection by CQL.
     *
     * @param <T> the type of the elements of the set to set.
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a prepared
     * variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a map type or if the elements of {@code v} are not of the type of
     * the elements of column {@code name}.
     * @throws NullPointerException if {@code v} contains null values. Nulls are not supported in collections
     * by CQL.
     */
    public <T> BoundStatement setSet(String name, Set<T> v) {
        return wrapper.setSet(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided UDT value.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this BoundStatement .
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this BoundStatement.
     * @throws InvalidTypeException if value {@code i} is not a UDT value or if its definition
     * does not correspond to the one of {@code v}.
     */
    public BoundStatement setUDTValue(int i, UDTValue v) {
        return wrapper.setUDTValue(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided UDT value.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this BoundStatement.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a UDT value or if the definition of column {@code name} does not correspond to
     * the one of {@code v}.
     */
    public BoundStatement setUDTValue(String name, UDTValue v) {
        return wrapper.setUDTValue(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided tuple value.
     *
     * @param i the index of the value to set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this BoundStatement.
     * @throws InvalidTypeException if value {@code i} is not a tuple value or if its types
     * do not correspond to the ones of {@code v}.
     */
    public BoundStatement setTupleValue(int i, TupleValue v) {
        return wrapper.setTupleValue(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided tuple value.
     *
     * @param name the name of the value to set; if {@code name} is present multiple
     * times, all its values are set.
     * @param v the value to set.
     * @return this BoundStatement.
     *
     * @throws IllegalArgumentException if {@code name} is not a valid name for this BoundStatement.
     * @throws InvalidTypeException if (any occurrence of) {@code name} is
     * not a tuple value or if the types of column {@code name} do not correspond to
     * the ones of {@code v}.
     */
    public BoundStatement setTupleValue(String name, TupleValue v) {
        return wrapper.setTupleValue(name, v);
    }

    /**
     * Sets the {@code i}th value to {@code null}.
     * <p>
     * This is mainly intended for CQL types which map to native Java types.
     *
     * @param i the index of the value to set.
     * @return this object.
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index for this object.
     */
    public BoundStatement setToNull(int i) {
        return wrapper.setToNull(i);
    }

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
    public BoundStatement setToNull(String name) {
        return wrapper.setToNull(name);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isNull(int i) {
        return wrapper.isNull(i);
    }

    /**
     * {@inheritDoc}
     */
    public boolean isNull(String name) {
        return wrapper.isNull(name);
    }

    /**
     * {@inheritDoc}
     */
    public boolean getBool(int i) {
        return wrapper.getBool(i);
    }

    /**
     * {@inheritDoc}
     */
    public boolean getBool(String name) {
        return wrapper.getBool(name);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByte(int i) {
        return wrapper.getByte(i);
    }

    /**
     * {@inheritDoc}
     */
    public byte getByte(String name) {
        return wrapper.getByte(name);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(int i) {
        return wrapper.getShort(i);
    }

    /**
     * {@inheritDoc}
     */
    public short getShort(String name) {
        return wrapper.getShort(name);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(int i) {
        return wrapper.getInt(i);
    }

    /**
     * {@inheritDoc}
     */
    public int getInt(String name) {
        return wrapper.getInt(name);
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(int i) {
        return wrapper.getLong(i);
    }

    /**
     * {@inheritDoc}
     */
    public long getLong(String name) {
        return wrapper.getLong(name);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(int i) {
        return wrapper.getDate(i);
    }

    /**
     * {@inheritDoc}
     */
    public Date getDate(String name) {
        return wrapper.getDate(name);
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(int i) {
        return wrapper.getFloat(i);
    }

    /**
     * {@inheritDoc}
     */
    public float getFloat(String name) {
        return wrapper.getFloat(name);
    }

    /**
     * {@inheritDoc}
     */
    public double getDouble(int i) {
        return wrapper.getDouble(i);
    }

    /**
     * {@inheritDoc}
     */
    public double getDouble(String name) {
        return wrapper.getDouble(name);
    }

    /**
     * {@inheritDoc}
     */
    public ByteBuffer getBytesUnsafe(int i) {
        return wrapper.getBytesUnsafe(i);
    }

    /**
     * {@inheritDoc}
     */
    public ByteBuffer getBytesUnsafe(String name) {
        return wrapper.getBytesUnsafe(name);
    }

    /**
     * {@inheritDoc}
     */
    public ByteBuffer getBytes(int i) {
        return wrapper.getBytes(i);
    }

    /**
     * {@inheritDoc}
     */
    public ByteBuffer getBytes(String name) {
        return wrapper.getBytes(name);
    }

    /**
     * {@inheritDoc}
     */
    public String getString(int i) {
        return wrapper.getString(i);
    }

    /**
     * {@inheritDoc}
     */
    public String getString(String name) {
        return wrapper.getString(name);
    }

    /**
     * {@inheritDoc}
     */
    public BigInteger getVarint(int i) {
        return wrapper.getVarint(i);
    }

    /**
     * {@inheritDoc}
     */
    public BigInteger getVarint(String name) {
        return wrapper.getVarint(name);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getDecimal(int i) {
        return wrapper.getDecimal(i);
    }

    /**
     * {@inheritDoc}
     */
    public BigDecimal getDecimal(String name) {
        return wrapper.getDecimal(name);
    }

    /**
     * {@inheritDoc}
     */
    public UUID getUUID(int i) {
        return wrapper.getUUID(i);
    }

    /**
     * {@inheritDoc}
     */
    public UUID getUUID(String name) {
        return wrapper.getUUID(name);
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress getInet(int i) {
        return wrapper.getInet(i);
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress getInet(String name) {
        return wrapper.getInet(name);
    }

    /**
     * {@inheritDoc}
     */
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        return wrapper.getList(i, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        return wrapper.getList(i, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return wrapper.getList(name, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        return wrapper.getList(name, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return wrapper.getSet(i, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        return wrapper.getSet(i, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return wrapper.getSet(name, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        return wrapper.getSet(name, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(i, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.getMap(i, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(name, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.getMap(name, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    public UDTValue getUDTValue(int i) {
        return wrapper.getUDTValue(i);
    }

    /**
     * {@inheritDoc}
     */
    public UDTValue getUDTValue(String name) {
        return wrapper.getUDTValue(name);
    }

    /**
     * {@inheritDoc}
     */
    public TupleValue getTupleValue(int i) {
        return wrapper.getTupleValue(i);
    }

    /**
     * {@inheritDoc}
     */
    public TupleValue getTupleValue(String name) {
        return wrapper.getTupleValue(name);
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(int i) {
        return wrapper.getObject(i);
    }

    /**
     * {@inheritDoc}
     */
    public Object getObject(String name) {
        return wrapper.getObject(name);
    }

    static class DataWrapper extends AbstractData<BoundStatement> {

        DataWrapper(BoundStatement wrapped, int size) {
            super(wrapped.statement.getPreparedId().protocolVersion, wrapped, size);
        }

        protected int[] getAllIndexesOf(String name) {
            return wrapped.statement.getVariables().getAllIdx(name);
        }

        protected DataType getType(int i) {
            return wrapped.statement.getVariables().getType(i);
        }

        protected String getName(int i) {
            return wrapped.statement.getVariables().getName(i);
        }
    }

    void ensureAllSet() {
        int index = 0;
        for (ByteBuffer value : wrapper.values) {
             if (value == BoundStatement.UNSET)
                throw new IllegalStateException("Unset value at index " + index + ". "
                                                + "If you want this value to be null, please set it to null explicitly.");
             index += 1;
        }
    }
}
