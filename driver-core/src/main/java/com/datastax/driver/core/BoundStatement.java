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

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * A prepared statement with values bound to the bind variables.
 * <p/>
 * Once values has been provided for the variables of the {@link PreparedStatement}
 * it has been created from, such BoundStatement can be executed (through
 * {@link Session#execute(Statement)}).
 * <p/>
 * The values of a BoundStatement can be set by either index or name. When
 * setting them by name, names follow the case insensitivity rules explained in
 * {@link ColumnDefinitions} but with the difference that if multiple bind
 * variables have the same name, setting that name will set <b>all</b> the
 * variables for that name.
 * <p/>
 * With native protocol V3 or below, all variables of the statement must be bound.
 * If you don't explicitly set a value for a variable, an {@code IllegalStateException}
 * will be thrown when submitting the statement. If you want to set a variable to
 * {@code null}, use {@link #setToNull(int) setToNull}.
 * <p/>
 * With native protocol V4 or above, variables can be left unset, in which case they
 * will be ignored server side (no tombstones will be generated). If you're reusing
 * a bound statement, you can {@link #unset(int) unset} variables that were previously
 * set.
 */
public class BoundStatement extends Statement implements SettableData<BoundStatement>, GettableData {
    static final ByteBuffer UNSET = ByteBuffer.allocate(0);

    final PreparedStatement statement;

    // Statement is already an abstract class, so we can't make it extend AbstractData directly. But
    // we still want to avoid duplicating too much code so we wrap.
    final DataWrapper wrapper;

    private final CodecRegistry codecRegistry;

    private ByteBuffer routingKey;

    /**
     * Creates a new {@code BoundStatement} from the provided prepared
     * statement.
     *
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
        if (statement.getOutgoingPayload() != null)
            this.setOutgoingPayload(statement.getOutgoingPayload());
        else
            // propagate incoming payload as outgoing payload, if no outgoing payload has been explicitly set
            this.setOutgoingPayload(statement.getIncomingPayload());
        this.codecRegistry = statement.getCodecRegistry();
        if (statement.isIdempotent() != null) {
            this.setIdempotent(statement.isIdempotent());
        }
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
     * @throws IllegalArgumentException if {@code name} is not a prepared
     *                                  variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public boolean isSet(String name) {
        return wrapper.getValue(wrapper.getIndexOf(name)) != UNSET;
    }

    /**
     * Unsets the {@code i}th variable. This will leave the statement in the same state as if no setter was
     * ever called for this variable.
     * <p/>
     * The treatment of unset variables depends on the native protocol version, see {@link BoundStatement}
     * for explanations.
     *
     * @param i the index of the variable.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     */
    public void unset(int i) {
        wrapper.setValue(i, UNSET);
    }

    /**
     * Unsets all occurrences of variable {@code name}.  This will leave the statement in the same state
     * as if no setter was ever called for this variable.
     * <p/>
     * The treatment of unset variables depends on the native protocol version, see {@link BoundStatement}
     * for explanations.
     *
     * @param name the name of the variable.
     * @throws IllegalArgumentException if {@code name} is not a prepared
     *                                  variable, that is if {@code !this.preparedStatement().variables().names().contains(name)}.
     */
    public void unset(String name) {
        for (int i : wrapper.getAllIndexesOf(name)) {
            wrapper.setValue(i, UNSET);
        }
    }

    /**
     * Bound values to the variables of this statement.
     * <p/>
     * This is a convenience method to bind all the variables of the
     * {@code BoundStatement} in one call.
     *
     * @param values the values to bind to the variables of the newly created
     *               BoundStatement. The first element of {@code values} will be bound to the
     *               first bind variable, etc. It is legal to provide fewer values than the
     *               statement has bound variables. In that case, the remaining variable need
     *               to be bound before execution. If more values than variables are provided
     *               however, an IllegalArgumentException wil be raised.
     * @return this bound statement.
     * @throws IllegalArgumentException if more {@code values} are provided
     *                                  than there is of bound variables in this statement.
     * @throws InvalidTypeException     if any of the provided value is not of
     *                                  correct type to be bound to the corresponding bind variable.
     * @throws NullPointerException     if one of {@code values} is a collection
     *                                  (List, Set or Map) containing a null value. Nulls are not supported in
     *                                  collections by CQL.
     */
    public BoundStatement bind(Object... values) {

        if (values.length > statement.getVariables().size())
            throw new IllegalArgumentException(String.format("Prepared statement has only %d variables, %d values provided", statement.getVariables().size(), values.length));

        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                wrapper.values[i] = null;
            } else {
                ProtocolVersion protocolVersion = statement.getPreparedId().protocolVersion;
                if (value instanceof Token)
                    // bypass CodecRegistry for token values
                    wrapper.values[i] = ((Token) value).serialize(protocolVersion);
                else
                    wrapper.values[i] = wrapper.codecFor(i, value).serialize(value, protocolVersion);
            }
        }
        return this;
    }

    /**
     * The routing key for this bound query.
     * <p/>
     * This method will return a non-{@code null} value if either of the following occur:
     * <ul>
     * <li>The routing key has been set directly through {@link BoundStatement#setRoutingKey}.</li>
     * <li>The routing key has been set through {@link PreparedStatement#setRoutingKey} for the
     * {@code PreparedStatement} this statement has been built from.</li>
     * <li>All the columns composing the partition key are bound variables of this {@code BoundStatement}. The routing
     * key will then be built using the values provided for these partition key columns.</li>
     * </ul>
     * Otherwise, {@code null} is returned.
     * <p/>
     * <p/>
     * Note that if the routing key has been set through {@link BoundStatement#setRoutingKey}, then that takes
     * precedence. If the routing key has been set through {@link PreparedStatement#setRoutingKey} then that is used
     * next. If neither of those are set then it is computed.
     *
     * @param protocolVersion unused by this implementation (no internal serialization is required to compute the key).
     * @param codecRegistry   unused by this implementation (no internal serialization is required to compute the key).
     * @return the routing key for this statement or {@code null}.
     */
    @Override
    public ByteBuffer getRoutingKey(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
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
     * Sets the routing key for this bound statement.
     * <p/>
     * This is useful when the routing key can neither be set on the {@code PreparedStatement} this bound statement
     * was built from, nor automatically computed from bound variables. In particular, this is the case if the
     * partition key is composite and only some of its components are bound.
     *
     * @param routingKey the raw (binary) value to use as routing key.
     * @return this {@code BoundStatement} object.
     * @see BoundStatement#getRoutingKey
     */
    public BoundStatement setRoutingKey(ByteBuffer routingKey) {
        this.routingKey = routingKey;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getKeyspace() {
        return statement.getPreparedId().metadata.size() == 0 ? null : statement.getPreparedId().metadata.getKeyspace(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBool(int i, boolean v) {
        return wrapper.setBool(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBool(String name, boolean v) {
        return wrapper.setBool(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setByte(int i, byte v) {
        return wrapper.setByte(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setByte(String name, byte v) {
        return wrapper.setByte(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setShort(int i, short v) {
        return wrapper.setShort(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setShort(String name, short v) {
        return wrapper.setShort(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setInt(int i, int v) {
        return wrapper.setInt(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setInt(String name, int v) {
        return wrapper.setInt(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setLong(int i, long v) {
        return wrapper.setLong(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setLong(String name, long v) {
        return wrapper.setLong(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTimestamp(int i, Date v) {
        return wrapper.setTimestamp(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTimestamp(String name, Date v) {
        return wrapper.setTimestamp(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDate(int i, LocalDate v) {
        return wrapper.setDate(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDate(String name, LocalDate v) {
        return wrapper.setDate(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTime(int i, long v) {
        return wrapper.setTime(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTime(String name, long v) {
        return wrapper.setTime(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setFloat(int i, float v) {
        return wrapper.setFloat(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setFloat(String name, float v) {
        return wrapper.setFloat(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDouble(int i, double v) {
        return wrapper.setDouble(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDouble(String name, double v) {
        return wrapper.setDouble(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setString(int i, String v) {
        return wrapper.setString(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setString(String name, String v) {
        return wrapper.setString(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBytes(int i, ByteBuffer v) {
        return wrapper.setBytes(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBytes(String name, ByteBuffer v) {
        return wrapper.setBytes(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
        return wrapper.setBytesUnsafe(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setBytesUnsafe(String name, ByteBuffer v) {
        return wrapper.setBytesUnsafe(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setVarint(int i, BigInteger v) {
        return wrapper.setVarint(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setVarint(String name, BigInteger v) {
        return wrapper.setVarint(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDecimal(int i, BigDecimal v) {
        return wrapper.setDecimal(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setDecimal(String name, BigDecimal v) {
        return wrapper.setDecimal(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setUUID(int i, UUID v) {
        return wrapper.setUUID(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setUUID(String name, UUID v) {
        return wrapper.setUUID(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setInet(int i, InetAddress v) {
        return wrapper.setInet(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setInet(String name, InetAddress v) {
        return wrapper.setInet(name, v);
    }

    /**
     * Sets the {@code i}th value to the provided {@link Token}.
     * <p/>
     * {@link #setPartitionKeyToken(Token)} should generally be preferred if you
     * have a single token variable.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this BoundStatement.
     * @throws IndexOutOfBoundsException if {@code i < 0 || i >= this.preparedStatement().variables().size()}.
     * @throws InvalidTypeException      if column {@code i} is not of the type of the token's value.
     */
    public BoundStatement setToken(int i, Token v) {
        return wrapper.setToken(i, v);
    }

    /**
     * Sets the value for (all occurrences of) variable {@code name} to the
     * provided token.
     * <p/>
     * {@link #setPartitionKeyToken(Token)} should generally be preferred if you
     * have a single token variable.
     * <p/>
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
     *             {@code name} are prepared, all of them are set.
     * @param v    the value to set.
     * @return this BoundStatement.
     * @throws IllegalArgumentException if {@code name} is not a prepared
     *                                  variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException     if (any occurrence of) {@code name} is
     *                                  not of the type of the token's value.
     */
    public BoundStatement setToken(String name, Token v) {
        return wrapper.setToken(name, v);
    }

    /**
     * Sets the value for (all occurrences of) variable "{@code partition key token}"
     * to the provided token (this is the name generated by Cassandra for markers
     * corresponding to a {@code token(...)} call).
     * <p/>
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
     * @throws IllegalArgumentException if {@code name} is not a prepared
     *                                  variable, that is, if {@code !this.preparedStatement().variables().names().contains(name)}.
     * @throws InvalidTypeException     if (any occurrence of) {@code name} is
     *                                  not of the type of the token's value.
     */
    public BoundStatement setPartitionKeyToken(Token v) {
        return setToken("partition key token", v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> BoundStatement setList(int i, List<T> v) {
        return wrapper.setList(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setList(int i, List<E> v, Class<E> elementsClass) {
        return wrapper.setList(i, v, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setList(int i, List<E> v, TypeToken<E> elementsType) {
        return wrapper.setList(i, v, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> BoundStatement setList(String name, List<T> v) {
        return wrapper.setList(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setList(String name, List<E> v, Class<E> elementsClass) {
        return wrapper.setList(name, v, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setList(String name, List<E> v, TypeToken<E> elementsType) {
        return wrapper.setList(name, v, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(int i, Map<K, V> v) {
        return wrapper.setMap(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.setMap(i, v, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.setMap(i, v, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(String name, Map<K, V> v) {
        return wrapper.setMap(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.setMap(name, v, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> BoundStatement setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.setMap(name, v, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> BoundStatement setSet(int i, Set<T> v) {
        return wrapper.setSet(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setSet(int i, Set<E> v, Class<E> elementsClass) {
        return wrapper.setSet(i, v, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setSet(int i, Set<E> v, TypeToken<E> elementsType) {
        return wrapper.setSet(i, v, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> BoundStatement setSet(String name, Set<T> v) {
        return wrapper.setSet(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setSet(String name, Set<E> v, Class<E> elementsClass) {
        return wrapper.setSet(name, v, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <E> BoundStatement setSet(String name, Set<E> v, TypeToken<E> elementsType) {
        return wrapper.setSet(name, v, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setUDTValue(int i, UDTValue v) {
        return wrapper.setUDTValue(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setUDTValue(String name, UDTValue v) {
        return wrapper.setUDTValue(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTupleValue(int i, TupleValue v) {
        return wrapper.setTupleValue(i, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setTupleValue(String name, TupleValue v) {
        return wrapper.setTupleValue(name, v);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(int i, V v, Class<V> targetClass) {
        return wrapper.set(i, v, targetClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(String name, V v, Class<V> targetClass) {
        return wrapper.set(name, v, targetClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(int i, V v, TypeToken<V> targetType) {
        return wrapper.set(i, v, targetType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(String name, V v, TypeToken<V> targetType) {
        return wrapper.set(name, v, targetType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(int i, V v, TypeCodec<V> codec) {
        return wrapper.set(i, v, codec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> BoundStatement set(String name, V v, TypeCodec<V> codec) {
        return wrapper.set(name, v, codec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setToNull(int i) {
        return wrapper.setToNull(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BoundStatement setToNull(String name) {
        return wrapper.setToNull(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int i) {
        return wrapper.isNull(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(String name) {
        return wrapper.isNull(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(int i) {
        return wrapper.getBool(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(String name) {
        return wrapper.getBool(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(int i) {
        return wrapper.getByte(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte getByte(String name) {
        return wrapper.getByte(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(int i) {
        return wrapper.getShort(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public short getShort(String name) {
        return wrapper.getShort(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int i) {
        return wrapper.getInt(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(String name) {
        return wrapper.getInt(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int i) {
        return wrapper.getLong(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(String name) {
        return wrapper.getLong(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp(int i) {
        return wrapper.getTimestamp(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp(String name) {
        return wrapper.getTimestamp(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate getDate(int i) {
        return wrapper.getDate(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LocalDate getDate(String name) {
        return wrapper.getDate(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTime(int i) {
        return wrapper.getTime(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTime(String name) {
        return wrapper.getTime(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int i) {
        return wrapper.getFloat(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(String name) {
        return wrapper.getFloat(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int i) {
        return wrapper.getDouble(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(String name) {
        return wrapper.getDouble(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        return wrapper.getBytesUnsafe(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return wrapper.getBytesUnsafe(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(int i) {
        return wrapper.getBytes(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(String name) {
        return wrapper.getBytes(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int i) {
        return wrapper.getString(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(String name) {
        return wrapper.getString(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(int i) {
        return wrapper.getVarint(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(String name) {
        return wrapper.getVarint(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(int i) {
        return wrapper.getDecimal(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(String name) {
        return wrapper.getDecimal(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(int i) {
        return wrapper.getUUID(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(String name) {
        return wrapper.getUUID(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(int i) {
        return wrapper.getInet(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(String name) {
        return wrapper.getInet(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        return wrapper.getList(i, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        return wrapper.getList(i, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return wrapper.getList(name, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getList(String name, TypeToken<T> elementsType) {
        return wrapper.getList(name, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        return wrapper.getSet(i, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        return wrapper.getSet(i, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return wrapper.getSet(name, elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Set<T> getSet(String name, TypeToken<T> elementsType) {
        return wrapper.getSet(name, elementsType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(i, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.getMap(i, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return wrapper.getMap(name, keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return wrapper.getMap(name, keysType, valuesType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UDTValue getUDTValue(int i) {
        return wrapper.getUDTValue(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UDTValue getUDTValue(String name) {
        return wrapper.getUDTValue(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleValue getTupleValue(int i) {
        return wrapper.getTupleValue(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleValue getTupleValue(String name) {
        return wrapper.getTupleValue(name);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int i) {
        return wrapper.getObject(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(String name) {
        return wrapper.getObject(name);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(int i, Class<T> targetClass) {
        return wrapper.get(i, targetClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(String name, Class<T> targetClass) {
        return wrapper.get(name, targetClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(int i, TypeToken<T> targetType) {
        return wrapper.get(i, targetType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(String name, TypeToken<T> targetType) {
        return wrapper.get(name, targetType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(int i, TypeCodec<T> codec) {
        return wrapper.get(i, codec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(String name, TypeCodec<T> codec) {
        return wrapper.get(name, codec);
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

        @Override
        protected CodecRegistry getCodecRegistry() {
            return wrapped.codecRegistry;
        }
    }
}
