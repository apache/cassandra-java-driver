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

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.*;
import static com.datastax.driver.core.DataType.Name.*;

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
 * Any variable that hasn't been specifically set will be considered {@code null}.
 * <p>
 * Bound values may also be retrieved using {@code get*()} methods. Note that this
 * may have a non-negligible impact on performance: internally, values are stored
 * in serialized form, so they need to be deserialized again. These methods are
 * provided for debugging purposes.
 */
public class BoundStatement extends Statement implements GettableData {

    final PreparedStatement statement;
    final ByteBuffer[] values;
    private final Cluster cluster;
    private ByteBuffer routingKey;

    private DataWrapper wrapper;

    /**
     * Creates a new {@code BoundStatement} from the provided prepared
     * statement.
     *
     * @deprecated use {@link #BoundStatement(PreparedStatement, Cluster)} instead.
     * @param statement the prepared statement from which to create a {@code BoundStatement}.
     */
    @Deprecated
    public BoundStatement(PreparedStatement statement){
        this(statement, null);
    }

    /**
     * Creates a new {@code BoundStatement} from the provided prepared
     * statement and using the provided cluster.
     *
     * @param statement the prepared statement from which to create a {@code BoundStatement}.
     * @param cluster the {@link Cluster} instance to use.
     */
    public BoundStatement(PreparedStatement statement, Cluster cluster) {
        this.statement = statement;
        this.cluster = cluster;
        this.values = new ByteBuffer[statement.getVariables().size()];

        // We want to reuse code from AbstractGettableData, but this class already extends Statement,
        // so we emulate a mixin with a delegate.
        this.wrapper = new DataWrapper(this);

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
        return isSet(metadata().getFirstIdx(name));
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
    @SuppressWarnings("unchecked")
    public BoundStatement bind(Object... values) {

        if (values.length > statement.getVariables().size())
            throw new IllegalArgumentException(String.format("Prepared statement has only %d variables, %d values provided", statement.getVariables().size(), values.length));

        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                setValue(i, null);
            } else {
                if (value instanceof Token)
                    value = ((Token)value).getValue();
                // use runtime inspection as the Java type is not known
                setValue(i, wrapper.codecFor(i, value).serialize(value));
            }
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
                return values[rkIndexes[0]];
            } else {
                ByteBuffer[] components = new ByteBuffer[rkIndexes.length];
                for (int i = 0; i < components.length; ++i) {
                    ByteBuffer value = values[rkIndexes[i]];
                    if (value == null)
                        return null;
                    components[i] = value;
                }
                return CodecUtils.compose(components);
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
        metadata().checkType(i, BOOLEAN);
        return setValue(i, wrapper.codecFor(i, Boolean.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            metadata().checkType(indexes[i], BOOLEAN);
            ByteBuffer value = wrapper.codecFor(i, Boolean.class).serialize(v);
            setValue(indexes[i], value);
        }
        return this;
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
        metadata().checkType(i, INT);
        return setValue(i, wrapper.codecFor(i, Integer.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setInt(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, BIGINT, COUNTER);
        return setValue(i, wrapper.codecFor(i, Long.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setLong(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, TIMESTAMP);
        return setValue(i, v == null ? null : wrapper.codecFor(i, Date.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setDate(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, FLOAT);
        return setValue(i, wrapper.codecFor(i, Float.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setFloat(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, DOUBLE);
        return setValue(i, wrapper.codecFor(i, Double.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setDouble(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, VARCHAR, TEXT, ASCII);
        return setValue(i, v == null ? null : wrapper.codecFor(i, String.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++)
            setString(indexes[i], v);
        return this;
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
        metadata().checkType(i, BLOB);
        return setValue(i, v == null ? null : wrapper.codecFor(i, ByteBuffer.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setBytes(indexes[i], v);
        }
        return this;
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
        int[] indexes = metadata().getAllIdx(name);
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i = 0; i < indexes.length; i++)
            setValue(indexes[i], value);
        return this;
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
        metadata().checkType(i, VARINT);
        return setValue(i, v == null ? null : wrapper.codecFor(i, BigInteger.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setVarint(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, DECIMAL);
        return setValue(i, v == null ? null : wrapper.codecFor(i, BigDecimal.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setDecimal(indexes[i], v);
        }
        return this;
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
        Name type = metadata().checkType(i, UUID, TIMEUUID);

        if (v == null)
            return setValue(i, null);

        if (type == TIMEUUID && v.version() != 1)
            throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));

        return setValue(i, wrapper.codecFor(i, UUID.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setUUID(indexes[i], v);
        }
        return this;
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
        metadata().checkType(i, INET);
        return setValue(i, v == null ? null : wrapper.codecFor(i, InetAddress.class).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++) {
            setInet(indexes[i], v);
        }
        return this;
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
    @SuppressWarnings("unchecked")
    public BoundStatement setToken(int i, Token v) {
        metadata().checkType(i, v.getType().getName());
        return setValue(i, getCodecRegistry().codecFor(v.getType(), v.getValue()).serialize(v.getValue()));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++)
            setToken(indexes[i], v);
        return this;
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
        metadata().checkType(i, LIST);

        if (v == null)
            return setValue(i, null);

        return setValue(i, wrapper.codecFor(i, v).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++)
            setList(indexes[i], v);
        return this;
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
        metadata().checkType(i, MAP);

        if (v == null)
            return setValue(i, null);

        return setValue(i, wrapper.codecFor(i, v).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int i = 0; i < indexes.length; i++)
            setMap(indexes[i], v);
        return this;
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
        metadata().checkType(i, SET);

        if (v == null)
            return setValue(i, null);

        return setValue(i, wrapper.codecFor(i, v).serialize(v));
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
        int[] indexes = metadata().getAllIdx(name);
        for (int index : indexes)
            setSet(index, v);
        return this;
    }

    public <T> BoundStatement setObject(int i, T value) {
        if(value == null) {
            setValue(i, null);
            return this;
        }
        TypeCodec<T> codec = wrapper.codecFor(i, value);
        return setValue(i, codec.serialize(value));
    }

    public <T> BoundStatement setObject(int i, T value, Class<T> targetClass) {
        return setObject(i, value, TypeToken.of(targetClass));
    }

    public <T> BoundStatement setObject(String name, T value, Class<T> targetClass) {
        int[] indexes = metadata().getAllIdx(name);
        for (int index : indexes)
            setObject(index, value,TypeToken.of(targetClass));
        return this;
    }

    public <T> BoundStatement setObject(int i, T value, TypeToken<T> targetType) {
        if(value == null) {
            setValue(i, null);
            return this;
        }
        TypeCodec<T> codec = wrapper.codecFor(i, targetType);
        return setValue(i, codec.serialize(value));
    }

    public <T> BoundStatement setObject(String name, T value, TypeToken<T> targetType) {
        int[] indexes = metadata().getAllIdx(name);
        for (int index : indexes)
            setObject(index, value, targetType);
        return this;
    }

    @Override
    public boolean isNull(int i) {
        return wrapper.isNull(i);
    }

    @Override
    public boolean isNull(String name) {
        return wrapper.isNull(name);
    }

    @Override
    public boolean getBool(int i) {
        return wrapper.getBool(i);
    }

    @Override
    public boolean getBool(String name) {
        return wrapper.getBool(name);
    }

    @Override
    public int getInt(int i) {
        return wrapper.getInt(i);
    }

    @Override
    public int getInt(String name) {
        return wrapper.getInt(name);
    }

    @Override
    public long getLong(int i) {
        return wrapper.getLong(i);
    }

    @Override
    public long getLong(String name) {
        return wrapper.getLong(name);
    }

    @Override
    public Date getDate(int i) {
        return wrapper.getDate(i);
    }

    @Override
    public Date getDate(String name) {
        return wrapper.getDate(name);
    }

    @Override
    public float getFloat(int i) {
        return wrapper.getFloat(i);
    }

    @Override
    public float getFloat(String name) {
        return wrapper.getFloat(name);
    }

    @Override
    public double getDouble(int i) {
        return wrapper.getDouble(i);
    }

    @Override
    public double getDouble(String name) {
        return wrapper.getDouble(name);
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        return wrapper.getBytesUnsafe(i);
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return wrapper.getBytesUnsafe(name);
    }

    @Override
    public ByteBuffer getBytes(int i) {
        return wrapper.getBytes(i);
    }

    @Override
    public ByteBuffer getBytes(String name) {
        return wrapper.getBytes(name);
    }

    @Override
    public String getString(int i) {
        return wrapper.getString(i);
    }

    @Override
    public String getString(String name) {
        return wrapper.getString(name);
    }

    @Override
    public BigInteger getVarint(int i) {
        return wrapper.getVarint(i);
    }

    @Override
    public BigInteger getVarint(String name) {
        return wrapper.getVarint(name);
    }

    @Override
    public BigDecimal getDecimal(int i) {
        return wrapper.getDecimal(i);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return wrapper.getDecimal(name);
    }

    @Override
    public UUID getUUID(int i) {
        return wrapper.getUUID(i);
    }

    @Override
    public UUID getUUID(String name) {
        return wrapper.getUUID(name);
    }

    @Override
    public InetAddress getInet(int i) {
        return wrapper.getInet(i);
    }

    @Override
    public InetAddress getInet(String name) {
        return wrapper.getInet(name);
    }

    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        return wrapper.getList(i, elementsClass);
    }

    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return wrapper.getList(name, elementsClass);
    }

    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return wrapper.getSet(i, elementsClass);
    }

    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return wrapper.getSet(name, elementsClass);
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(i, keysClass, valuesClass);
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(name, keysClass, valuesClass);
    }

    @Override
    public Object getObject(int i) {
        return wrapper.getObject(i);
    }

    @Override
    public Object getObject(String name) {
        return wrapper.getObject(name);
    }

    @Override
    public <T> T getObject(int i, Class<T> targetClass) {
        return wrapper.getObject(i, targetClass);
    }

    @Override
    public <T> T getObject(int i, TypeToken<T> targetType) {
        return wrapper.getObject(i, targetType);
    }

    @Override
    public <T> T getObject(String name, Class<T> targetClass) {
        return wrapper.getObject(name, targetClass);
    }

    @Override
    public <T> T getObject(String name, TypeToken<T> targetType) {
        return wrapper.getObject(name, targetType);
    }

    private ColumnDefinitions metadata() {
        return statement.getVariables();
    }

    private BoundStatement setValue(int i, ByteBuffer value) {
        values[i] = value;
        return this;
    }

    private CodecRegistry getCodecRegistry() {
        return cluster.getConfiguration().getCodecRegistry();
    }

    class DataWrapper extends AbstractGettableData {
        final ByteBuffer[] values;

        DataWrapper(BoundStatement wrapped) {
            super(wrapped.metadata(), wrapped.getCodecRegistry());
            values = wrapped.values;
        }

        @Override
        protected ByteBuffer getValue(int i) {
            return values[i];
        }

    }
}
