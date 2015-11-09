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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayListWithCapacity;

import static com.datastax.driver.core.TypeTokens.listOf;
import static com.datastax.driver.core.TypeTokens.mapOf;
import static com.datastax.driver.core.TypeTokens.setOf;

/**
 * A regular (non-prepared and non batched) CQL statement.
 * <p>
 * This class represents a query string along with query options (and optionally
 * associated values, see {@link #getValueDefinitions()}). It can be extended but {@link SimpleStatement}
 * is provided as a simple implementation to build a {@code RegularStatement} directly
 * from its query string.
 */
public abstract class RegularStatement extends Statement implements GettableData, SettableData<RegularStatement> {

    private enum ParameterMode {
        POSITIONAL, NAMED
    }

    protected final ProtocolVersion protocolVersion;

    protected final CodecRegistry codecRegistry;

    private final NavigableMap<Object, Value> values = new TreeMap<Object, Value>();

    private ParameterMode parameterMode = null;

    protected RegularStatement(ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        this.protocolVersion = protocolVersion;
        this.codecRegistry = codecRegistry;
    }

    /**
     * Returns the query string for this statement.
     *
     * @return a valid CQL query string.
     */
    public abstract String getQueryString();

    /**
     * Returns the definitions of the values that were set on this statement.
     * <p>
     * This information is based solely on the methods that were used to set values on the statement.
     * In particular, there is no guarantee that the variables match the placeholders in the query string.
     * The only guarantee is that variables of a given statement are either all named or all positional, and
     * that if they are positional, the list will be sorted by ascending index.
     * <p>
     * To retrieve a given value, use the getter methods on this class.
     * <p>
     * Note: Values for a RegularStatement (i.e. if this method does not return
     * an empty list) are not supported with the native protocol version 1: you
     * will get an {@link UnsupportedProtocolVersionException} when submitting
     * one if version 1 of the protocol is in use (i.e. if you've forced version
     * 1 through {@link Cluster.Builder#withProtocolVersion} or you use
     * Cassandra 1.2).
     *
     * @return the list of definitions.
     */
    public List<ValueDefinition> getValueDefinitions() {
        maybeRefreshValues();
        List<ValueDefinition> definitions = Lists.newArrayListWithCapacity(values.size());
        for (Value value : values.values()) {
            definitions.add(value.definition);
        }
        return definitions;
    }

    /**
     * Returns the number of values that were set on this statement. If you only need this
     * number, this is slightly more efficient than {@code getValueDefinitions().size()},
     * which does an internal transformation.
     *
     * @return the number of values.
     */
    public int valuesCount() {
        maybeRefreshValues();
        return values.size();
    }

    /**
     * Returns the serialized values to send along with the statement (this is called
     * internally by the driver when the statement is executed).
     *
     * @return the values, or an empty list if this statement has no values.
     */
    protected List<ByteBuffer> getValues() {
        maybeRefreshValues();
        List<ByteBuffer> bbs;
        if (parameterMode == null) {
            assert values.isEmpty();
            return Collections.emptyList();
        }
        switch (parameterMode) {
            case NAMED:
                // Might still happen if a value was set and then unset
                if (values.isEmpty())
                    return Collections.emptyList();

                bbs = newArrayListWithCapacity(values.size());
                for (Value value : values.values()) {
                    bbs.add(value.bytes);
                }
                return bbs;
            case POSITIONAL:
                if (values.isEmpty())
                    return Collections.emptyList();

                // If there are gaps in the indices, we need to mark them unset
                int last = (Integer)(values.lastEntry().getKey());
                bbs = newArrayListWithCapacity(last + 1);
                for (int i = 0; i <= last; i++) {
                    Value value = values.get(i);
                    bbs.add(value == null ? CBUtil.UNSET_VALUE : value.bytes);
                }
                return bbs;
            default:
                throw new AssertionError("Unsupported parameter mode " + parameterMode);
        }
    }

    /**
     * Returns the values names to send along with the statement (this is called
     * internally by the driver when the statement is executed).
     *
     * @return the value names, or an empty list if there are no values in this statement,
     * or if positional parameters are being used.
     */
    protected List<String> getValueNames() {
        maybeRefreshValues();
        if (parameterMode != ParameterMode.NAMED)
            return Collections.emptyList();
        List<String> names = newArrayListWithCapacity(values.size());
        for (Object value : values.keySet()) {
            names.add((String)value);
        }
        return names;
    }

    /**
     * Whether or not this statement has values, that is if {@link #getValues()}
     * will return an empty list or not.
     *
     * @return {@code false} if {@link #getValues()} returns empty, {@code true}
     * otherwise.
     */
    public boolean hasValues() {
        maybeRefreshValues();
        return !values.isEmpty();
    }

    /**
     * Whether this statement uses positional values.
     *
     * @return {@code true} if this statement has values and the values are positional, {@code false} otherwise.
     */
    public boolean usesPositionalValues() {
        maybeRefreshValues();
        return parameterMode == ParameterMode.POSITIONAL;
    }

    /**
     * Whether this statement uses named values.
     *
     * @return {@code true} if this statement has values and the values are named, {@code false} otherwise.
     */
    public boolean usesNamedValues() {
        maybeRefreshValues();
        return parameterMode == ParameterMode.NAMED;
    }

    /**
     * Bind values to the variables of this statement.
     *
     * This is a convenience method to bind all the variables of the
     * {@code RegularStatement} in one call.
     *
     * @param values the values to bind to the variables of the newly created
     * RegularStatement. The first element of {@code values} will be bound to the
     * first bind variable, etc. It is legal to provide fewer values than the
     * statement has bound variables. In that case, the remaining variables need
     * to be bound before execution.
     *
     * @return this statement.
     *
     */
    public RegularStatement bind(Object... values) {
        this.values.clear();
        this.parameterMode = null;
        if (values != null) {
            if (values.length > 65535)
                throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");
            for (int i = 0; i < values.length; i++) {
                Object value = values[i];
                if (value == null) {
                    // impossible to locate the right codec when object is null,
                    // so forcing the result to null
                    setToNull(i);
                } else {
                    if (value instanceof Token) {
                        // bypass CodecRegistry for Token instances
                        setToken(i, (Token)value);
                    } else {
                        set(i, value, codecRegistry.codecFor(value));
                    }
                }
            }
        }
        return this;
    }

    /**
     * Returns whether the {@code i}th variable has been bound.
     *
     * @param i the index of the variable to check.
     * @return whether the {@code i}th variable has been bound.
     */
    public boolean isSet(int i) {
        checkParameterMode(i);
        return values.containsKey(i);
    }

    /**
     * Returns whether the variable {@code name} has been
     * bound.
     *
     * @param name the name of the variable to check.
     * @return whether the variable {@code name} has been bound.
     */
    public boolean isSet(String name) {
        checkParameterMode(name);
        return values.containsKey(name);
    }

    /**
     * Unsets the {@code i}th variable. This will leave the statement in the same state as if no setter was
     * ever called for this variable.
     * <p>
     * The treatment of unset variables depends on the native protocol version, see {@link BoundStatement}
     * for explanations.
     *
     * @param i the index of the variable.
     */
    public void unset(int i) {
        checkParameterMode(i);
        values.remove(i);
    }

    /**
     * Unsets the variable {@code name}. This will leave the statement in the same state
     * as if no setter was ever called for this variable.
     * <p>
     * The treatment of unset variables depends on the native protocol version, see {@link BoundStatement}
     * for explanations.
     *
     * @param name the name of the variable.
     */
    public void unset(String name) {
        checkParameterMode(name);
        values.remove(name);
    }

    @Override
    public RegularStatement setToNull(int i) {
        return setInternal(i, new Value(i, null, null));
    }

    @Override
    public RegularStatement setToNull(String name) {
        return setInternal(name, new Value(name, null, null));
    }

    @Override
    public boolean isNull(int i) {
        return getInternal(i).bytes == null;
    }

    @Override
    public boolean isNull(String name) {
        return getInternal(name).bytes == null;
    }

    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        return getInternal(i).bytes;
    }

    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return getInternal(name).bytes;
    }

    @Override
    public RegularStatement setBytesUnsafe(int i, ByteBuffer v) {
        return setInternal(i, new Value(i, null, v));
    }

    @Override
    public RegularStatement setBytesUnsafe(String name, ByteBuffer v) {
        return setInternal(name, new Value(name, null, v));
    }

    @Override
    public ByteBuffer getBytes(int i) {
        return get(i, ByteBuffer.class);
    }

    @Override
    public ByteBuffer getBytes(String name) {
        return get(name, ByteBuffer.class);
    }

    @Override
    public RegularStatement setBytes(int i, ByteBuffer v) {
        return set(i, v, ByteBuffer.class);
    }

    @Override
    public RegularStatement setBytes(String name, ByteBuffer v) {
        return set(name, v, ByteBuffer.class);
    }

    @Override
    public boolean getBool(int i) {
        return get(i, Boolean.class);
    }

    @Override
    public boolean getBool(String name) {
        return get(name, Boolean.class);
    }

    @Override
    public RegularStatement setBool(int i, boolean v) {
        return set(i, v, Boolean.class);
    }

    @Override
    public RegularStatement setBool(String name, boolean v) {
        return set(name, v, Boolean.class);
    }

    @Override
    public byte getByte(int i) {
        return get(i, Byte.class);
    }

    @Override
    public byte getByte(String name) {
        return get(name, Byte.class);
    }

    @Override
    public RegularStatement setByte(int i, byte v) {
        return set(i, v, Byte.class);
    }

    @Override
    public RegularStatement setByte(String name, byte v) {
        return set(name, v, Byte.class);
    }

    @Override
    public short getShort(int i) {
        return get(i, Short.class);
    }

    @Override
    public short getShort(String name) {
        return get(name, Short.class);
    }

    @Override
    public RegularStatement setShort(int i, short v) {
        return set(i, v, Short.class);
    }

    @Override
    public RegularStatement setShort(String name, short v) {
        return set(name, v, Short.class);
    }

    @Override
    public int getInt(int i) {
        return get(i, Integer.class);
    }

    @Override
    public int getInt(String name) {
        return get(name, Integer.class);
    }

    @Override
    public RegularStatement setInt(int i, int v) {
        return set(i, v, Integer.class);
    }

    @Override
    public RegularStatement setInt(String name, int v) {
        return set(name, v, Integer.class);
    }

    @Override
    public long getLong(int i) {
        return get(i, Long.class);
    }

    @Override
    public long getLong(String name) {
        return get(name, Long.class);
    }

    @Override
    public RegularStatement setLong(int i, long v) {
        return set(i, v, Long.class);
    }

    @Override
    public RegularStatement setLong(String name, long v) {
        return set(name, v, Long.class);
    }

    @Override
    public float getFloat(int i) {
        return get(i, Float.class);
    }

    @Override
    public float getFloat(String name) {
        return get(name, Float.class);
    }

    @Override
    public RegularStatement setFloat(int i, float v) {
        return set(i, v, Float.class);
    }

    @Override
    public RegularStatement setFloat(String name, float v) {
        return set(name, v, Float.class);
    }

    @Override
    public double getDouble(int i) {
        return get(i, Double.class);
    }

    @Override
    public double getDouble(String name) {
        return get(name, Double.class);
    }

    @Override
    public RegularStatement setDouble(int i, double v) {
        return set(i, v, Double.class);
    }

    @Override
    public RegularStatement setDouble(String name, double v) {
        return set(name, v, Double.class);
    }

    @Override
    public BigInteger getVarint(int i) {
        return get(i, BigInteger.class);
    }

    @Override
    public BigInteger getVarint(String name) {
        return get(name, BigInteger.class);
    }

    @Override
    public RegularStatement setVarint(int i, BigInteger v) {
        return set(i, v, BigInteger.class);
    }

    @Override
    public RegularStatement setVarint(String name, BigInteger v) {
        return set(name, v, BigInteger.class);
    }

    @Override
    public BigDecimal getDecimal(int i) {
        return get(i, BigDecimal.class);
    }

    @Override
    public BigDecimal getDecimal(String name) {
        return get(name, BigDecimal.class);
    }

    @Override
    public RegularStatement setDecimal(int i, BigDecimal v) {
        return set(i, v, BigDecimal.class);
    }

    @Override
    public RegularStatement setDecimal(String name, BigDecimal v) {
        return set(name, v, BigDecimal.class);
    }

    @Override
    public String getString(int i) {
        return get(i, String.class);
    }

    @Override
    public String getString(String name) {
        return get(name, String.class);
    }

    @Override
    public RegularStatement setString(int i, String v) {
        return set(i, v, String.class);
    }

    @Override
    public RegularStatement setString(String name, String v) {
        return set(name, v, String.class);
    }

    @Override
    public Date getTimestamp(int i) {
        return get(i, Date.class);
    }

    @Override
    public Date getTimestamp(String name) {
        return get(name, Date.class);
    }

    @Override
    public RegularStatement setTimestamp(int i, Date v) {
        return set(i, v, Date.class);
    }

    @Override
    public RegularStatement setTimestamp(String name, Date v) {
        return set(name, v, Date.class);
    }

    @Override
    public long getTime(int i) {
        return getLong(i);
    }

    @Override
    public long getTime(String name) {
        return getLong(name);
    }

    @Override
    public RegularStatement setTime(int i, long v) {
        return setLong(i, v);
    }

    @Override
    public RegularStatement setTime(String name, long v) {
        return setLong(name, v);
    }

    @Override
    public LocalDate getDate(int i) {
        return get(i, LocalDate.class);
    }

    @Override
    public LocalDate getDate(String name) {
        return get(name, LocalDate.class);
    }

    @Override
    public RegularStatement setDate(int i, LocalDate v) {
        return set(i, v, LocalDate.class);
    }

    @Override
    public RegularStatement setDate(String name, LocalDate v) {
        return set(name, v, LocalDate.class);
    }

    @Override
    public UUID getUUID(int i) {
        return get(i, UUID.class);
    }

    @Override
    public UUID getUUID(String name) {
        return get(name, UUID.class);
    }

    @Override
    public RegularStatement setUUID(int i, UUID v) {
        return set(i, v, UUID.class);
    }

    @Override
    public RegularStatement setUUID(String name, UUID v) {
        return set(name, v, UUID.class);
    }

    @Override
    public InetAddress getInet(int i) {
        return get(i, InetAddress.class);
    }

    @Override
    public InetAddress getInet(String name) {
        return get(name, InetAddress.class);
    }

    @Override
    public RegularStatement setInet(int i, InetAddress v) {
        return set(i, v, InetAddress.class);
    }

    @Override
    public RegularStatement setInet(String name, InetAddress v) {
        return set(name, v, InetAddress.class);
    }

    @Override
    public UDTValue getUDTValue(int i) {
        return get(i, UDTValue.class);
    }

    @Override
    public UDTValue getUDTValue(String name) {
        return get(name, UDTValue.class);
    }

    @Override
    public RegularStatement setUDTValue(int i, UDTValue v) {
        return set(i, v, UDTValue.class);
    }

    @Override
    public RegularStatement setUDTValue(String name, UDTValue v) {
        return set(name, v, UDTValue.class);
    }

    @Override
    public TupleValue getTupleValue(int i) {
        return get(i, TupleValue.class);
    }

    @Override
    public TupleValue getTupleValue(String name) {
        return get(name, TupleValue.class);
    }

    @Override
    public RegularStatement setTupleValue(int i, TupleValue v) {
        return set(i, v, TupleValue.class);
    }

    @Override
    public RegularStatement setTupleValue(String name, TupleValue v) {
        return set(name, v, TupleValue.class);
    }

    @Override
    public <V> List<V> getList(int i, Class<V> elementsClass) {
        return getList(i, TypeToken.of(elementsClass));
    }

    @Override
    public <V> List<V> getList(String name, Class<V> elementsClass) {
        return getList(name, TypeToken.of(elementsClass));
    }

    @Override
    public <V> List<V> getList(int i, TypeToken<V> elementsType) {
        return get(i, listOf(elementsType));
    }

    @Override
    public <V> List<V> getList(String name, TypeToken<V> elementsType) {
        return get(name, listOf(elementsType));
    }

    @Override
    public <E> RegularStatement setList(int i, List<E> v) {
        if (v == null)
            setToNull(i);
        return set(i, v, codecRegistry.codecFor(v));
    }

    @Override
    public <E> RegularStatement setList(String name, List<E> v) {
        if (v == null)
            setToNull(name);
        return set(name, v, codecRegistry.codecFor(v));
    }

    @Override
    public <E> RegularStatement setList(int i, List<E> v, Class<E> elementsClass) {
        return setList(i, v, TypeToken.of(elementsClass));
    }

    @Override
    public <E> RegularStatement setList(String name, List<E> v, Class<E> elementsClass) {
        return setList(name, v, TypeToken.of(elementsClass));
    }

    @Override
    public <E> RegularStatement setList(int i, List<E> v, TypeToken<E> elementsType) {
        return set(i, v, listOf(elementsType));
    }

    @Override
    public <E> RegularStatement setList(String name, List<E> v, TypeToken<E> elementsType) {
        return set(name, v, listOf(elementsType));
    }

    @Override
    public <V> Set<V> getSet(int i, Class<V> elementsClass) {
        return getSet(i, TypeToken.of(elementsClass));
    }

    @Override
    public <V> Set<V> getSet(String name, Class<V> elementsClass) {
        return getSet(name, TypeToken.of(elementsClass));
    }

    @Override
    public <V> Set<V> getSet(int i, TypeToken<V> elementsType) {
        return get(i, setOf(elementsType));
    }

    @Override
    public <V> Set<V> getSet(String name, TypeToken<V> elementsType) {
        return get(name, setOf(elementsType));
    }

    @Override
    public <E> RegularStatement setSet(int i, Set<E> v) {
        if (v == null)
            setToNull(i);
        return set(i, v, codecRegistry.codecFor(v));
    }

    @Override
    public <E> RegularStatement setSet(String name, Set<E> v) {
        if (v == null)
            setToNull(name);
        return set(name, v, codecRegistry.codecFor(v));
    }

    @Override
    public <E> RegularStatement setSet(int i, Set<E> v, Class<E> elementsClass) {
        return setSet(i, v, TypeToken.of(elementsClass));
    }

    @Override
    public <E> RegularStatement setSet(String name, Set<E> v, Class<E> elementsClass) {
        return setSet(name, v, TypeToken.of(elementsClass));
    }

    @Override
    public <E> RegularStatement setSet(int i, Set<E> v, TypeToken<E> elementsType) {
        return set(i, v, setOf(elementsType));
    }

    @Override
    public <E> RegularStatement setSet(String name, Set<E> v, TypeToken<E> elementsType) {
        return set(name, v, setOf(elementsType));
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(i, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(name, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    @Override
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return get(i, mapOf(keysType, valuesType));
    }

    @Override
    public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return get(name, mapOf(keysType, valuesType));
    }

    @Override
    public <K, V> RegularStatement setMap(int i, Map<K, V> v) {
        if (v == null)
            setToNull(i);
        return set(i, v, codecRegistry.codecFor(v));
    }

    @Override
    public <K, V> RegularStatement setMap(String name, Map<K, V> v) {
        if (v == null)
            setToNull(name);
        return set(name, v, codecRegistry.codecFor(v));
    }

    @Override
    public <K, V> RegularStatement setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
        return setMap(i, v, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    @Override
    public <K, V> RegularStatement setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
        return setMap(name, v, TypeToken.of(keysClass), TypeToken.of(valuesClass));
    }

    @Override
    public <K, V> RegularStatement setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return set(i, v, mapOf(keysType, valuesType));
    }

    @Override
    public <K, V> RegularStatement setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return set(name, v, mapOf(keysType, valuesType));
    }

    @Override
    public Object getObject(int i) {
        return getInternal(i).asObject();
    }

    @Override
    public Object getObject(String name) {
        return getInternal(name).asObject();
    }

    @Override
    public <V> V get(int i, Class<V> targetClass) {
        return get(i, TypeToken.of(targetClass));
    }

    @Override
    public <V> V get(String name, Class<V> targetClass) {
        return get(name, TypeToken.of(targetClass));
    }

    @Override
    public <V> V get(int i, TypeToken<V> targetType) {
        Preconditions.checkNotNull(targetType, "Target type must not be null");
        Value value = getInternal(i);
        return value.as(targetType);
    }

    @Override
    public <V> V get(String name, TypeToken<V> targetType) {
        Preconditions.checkNotNull(targetType, "Target type must not be null");
        Value value = getInternal(name);
        return value.as(targetType);
    }

    @Override
    public <V> V get(int i, TypeCodec<V> codec) {
        Value value = getInternal(i);
        return value.with(codec);
    }

    @Override
    public <V> V get(String name, TypeCodec<V> codec) {
        Value value = getInternal(name);
        return value.with(codec);
    }

    @Override
    public <V> RegularStatement set(int i, V v, Class<V> targetClass) {
        return set(i, v, TypeToken.of(targetClass));
    }

    @Override
    public <V> RegularStatement set(String name, V v, Class<V> targetClass) {
        return set(name, v, TypeToken.of(targetClass));
    }

    @Override
    public <V> RegularStatement set(int i, V v, TypeToken<V> targetType) {
        // we must treat tuples and UDTs separately
        // as they convey their own specific CQL types as well
        DataType cqlType = null;
        if (v instanceof TupleValue)
            cqlType = ((TupleValue)v).getType();
        if (v instanceof UDTValue)
            cqlType = ((UDTValue)v).getType();
        return set(i, v, codecRegistry.codecFor(cqlType, targetType));
    }

    @Override
    public <V> RegularStatement set(String name, V v, TypeToken<V> targetType) {
        // we must treat tuples and UDTs separately
        // as they convey their own specific CQL types as well
        DataType cqlType = null;
        if (v instanceof TupleValue)
            cqlType = ((TupleValue)v).getType();
        if (v instanceof UDTValue)
            cqlType = ((UDTValue)v).getType();
        return set(name, v, codecRegistry.codecFor(cqlType, targetType));
    }

    @Override
    public <V> RegularStatement set(int i, V v, TypeCodec<V> codec) {
        ByteBuffer bytes = codec.serialize(v, protocolVersion);
        return setInternal(i, new Value(i, codec.getCqlType(), bytes));
    }

    @Override
    public <V> RegularStatement set(String name, V v, TypeCodec<V> codec) {
        ByteBuffer bytes = codec.serialize(v, protocolVersion);
        return setInternal(name, new Value(name, codec.getCqlType(), bytes));
    }

    /**
     * Sets the {@code i}th value to the provided {@link Token}.
     * <p>
     * {@link #setPartitionKeyToken(Token)} should generally be preferred if you
     * have a single token variable.
     *
     * @param i the index of the variable to set.
     * @param v the value to set.
     * @return this RegularStatement.
     */
    public RegularStatement setToken(int i, Token v) {
        if (v == null)
            throw new NullPointerException(String.format("Cannot set a null token for parameter %s", i));
        // Bypass CodecRegistry when serializing tokens
        return setBytesUnsafe(i, v.serialize(protocolVersion));
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
     * SimpleStatement st = session.newSimpleStatement("SELECT * FROM my_table WHERE token(k) > :min AND token(k) <= :max");
     * st.setToken("min", minToken).setToken("max", maxToken);
     * }
     * </pre>
     *
     * @param name the name of the variable to set; if multiple variables
     * {@code name} are prepared, all of them are set.
     * @param v the value to set.
     * @return this RegularStatement.
     */
    public RegularStatement setToken(String name, Token v) {
        if (v == null)
            throw new NullPointerException(String.format("Cannot set a null token for parameter %s", name));
        // Bypass CodecRegistry when serializing tokens
        return setBytesUnsafe(name, v.serialize(protocolVersion));
    }

    /**
     * Sets the value for variable "{@code partition key token}"
     * to the provided token (this is the name generated by Cassandra for markers
     * corresponding to a {@code token(...)} call).
     * <p>
     * This method is a shorthand for statements with a single token variable:
     * <pre>
     * {@code
     * Token token = ...
     * SimpleStatement st = session.newSimpleStatement("SELECT * FROM my_table WHERE token(k) = ?");
     * st.setPartitionKeyToken(token);
     * }
     * </pre>
     * If you have multiple token variables, use positional binding ({@link #setToken(int, Token)},
     * or named bind markers:
     * <pre>
     * {@code
     * SimpleStatement st = session.newSimpleStatement("SELECT * FROM my_table WHERE token(k) > :min AND token(k) <= :max");
     * st.setToken("min", minToken).setToken("max", maxToken);
     * }
     * </pre>
     *
     * @param v the value to set.
     * @return this RegularStatement.
     */
    public RegularStatement setPartitionKeyToken(Token v) {
        return setToken("partition key token", v);
    }

    private Value getInternal(Object key) {
        Preconditions.checkNotNull(key, "Parameter name must not be null");
        checkParameterMode(key);
        maybeRefreshValues();
        checkArgument(values.containsKey(key), "Parameter not set: %s", key);
        return values.get(key);
    }

    private RegularStatement setInternal(Object key, Value value) {
        checkParameterMode(key);

        if (!values.containsKey(key) && values.size() == 65535)
            throw new IllegalArgumentException("Too many values, the maximum allowed is 65535");

        values.put(key, value);
        return this;
    }

    /**
     * Copies the values of another statement into this statement.
     * <p>
     * Both statements must use positional values (or no values at all). If this statement already contains values, the new
     * values will be inserted starting at the index following the current max index.
     *
     * @param that the other statement.
     */
    protected void copyValues(RegularStatement that) {
        if (!that.hasValues())
            return;

        // We don't allow named values because we'd have no way to handle colliding names
        Preconditions.checkArgument(parameterMode != ParameterMode.NAMED && that.usesPositionalValues());

        int offset = values.isEmpty()
            ? 0
            : (Integer)(values.lastEntry().getKey()) + 1;

        for (ValueDefinition definition : that.getValueDefinitions()) {
            int thatIndex = definition.getIndex();
            assert thatIndex >= 0; // from usesPositionalValues check above
            int thisIndex = offset + thatIndex;
            setInternal(thisIndex, new Value(thisIndex, definition.getType(), that.getBytesUnsafe(thatIndex)));
        }
    }

    /**
     * Refreshes the statement's values.
     * <p>
     * This method is intended for implementations that generate the values dynamically. It's called at the beginning of each method that
     * provides access to the values ({@link #getValueDefinitions()}, all individual getters, etc.). Subclasses can hook into this method
     * to implement a caching strategy, in order to only regenerate the values when needed. This avoids having to override each and
     * every {@code getXxx} method.
     * <p>
     * The default implementation does nothing.
     */
    protected void maybeRefreshValues() {
    }

    private void checkParameterMode(Object key) {
        ParameterMode parameterMode = key instanceof Integer ? ParameterMode.POSITIONAL : ParameterMode.NAMED;
        if (this.parameterMode == null)
            this.parameterMode = parameterMode;
        else
            checkArgument(this.parameterMode == parameterMode, "Cannot mix positional and named parameters in the same statement");
    }

    /**
     * Metadata describing a value associated with a {@link RegularStatement}.
     * <p>
     * Note that this information is based solely on the methods that were used to set values on the statement.
     * In particular, there is no validation that the value names or indices match the placeholders in the statement's
     * query string, or that the data types correspond to the actual database schema.
     * Regular statements are only validated on the server side.
     */
    public static class ValueDefinition {
        private final int index;
        private final String name;
        private final DataType type;

        ValueDefinition(int index, DataType type) {
            this(index, null, type);
        }

        ValueDefinition(String name, DataType type) {
            this(-1, name, type);
        }

        private ValueDefinition(int index, String name, DataType type) {
            this.index = index;
            this.name = name;
            this.type = type;
        }

        /**
         * Return the index of the value in the statement.
         * <p>
         * This information is set if a positional setter (for example {@link #setDouble(int, double)}) was used to set the value.
         * Note that the variables of a given statement are either all named or all positional.
         *
         * @return the index, or -1 if named variables were used on this statement.
         */
        public int getIndex() {
            return index;
        }

        /**
         * Return the name of the value in the statement.
         * <p>
         * This information is set if a named setter (for example {@link #setDouble(String, double)}) was used to set the value.
         * Note that the variables of a given statement are either all named or all positional.
         *
         * @return the name, or {@code null} if positional variables were used on this statement.
         */
        public String getName() {
            return name;
        }

        /**
         * Return the CQL data type inferred for the value.
         * <p>
         * This information is determined from the method that was used to set the value. For example, if
         * {@link #setDouble(String, double)} was used, it will be {@link DataType#cdouble()}. For non-standard types, it will
         * be the CQL type of the first {@link CodecRegistry custom codec} that can handle the value.
         * The only case where the CQL type can't be inferred is if the value was set with {@link #setBytesUnsafe(int, ByteBuffer)}
         * or {@link #setToNull(int)}; then this method will return {@code null}.
         *
         * @return the data type, or {@code null} if this variable was set with {@link #setBytesUnsafe(int, ByteBuffer)}
         * or {@link #setToNull(int)}.
         */
        public DataType getType() {
            return type;
        }

        @Override
        public String toString() {
            return String.format("%s:%s",
                (index >= 0) ? index : name,
                (type == null) ? "<unknown type>" : type.toString());
        }
    }

    // A value that was set on the statement.
    private class Value {

        private final ValueDefinition definition;

        // The serialized form that will be sent to Cassandra alongside the query
        private final ByteBuffer bytes;

        private Value(int index, DataType type, ByteBuffer bytes) {
            this(new ValueDefinition(index, type), bytes);
        }

        private Value(String name, DataType type, ByteBuffer bytes) {
            this(new ValueDefinition(name, type), bytes);
        }

        private Value(ValueDefinition definition, ByteBuffer bytes) {
            this.definition = definition;
            this.bytes = bytes;
        }

        private <V> V as(TypeToken<V> javaType) {
            return with(codecFor(javaType));
        }

        private <V> V with(TypeCodec<V> codec) {
            return codec.deserialize(bytes, protocolVersion);
        }

        private <V> TypeCodec<V> codecFor(TypeToken<V> javaType) {
            return codecRegistry.codecFor(definition.type, javaType);
        }

        private Object asObject() {
            // If the value was set with setBytesUnsafe, we don't have an original object to return, so by default
            // return a copy of the buffer itself.
            if (definition.type == null)
                return bytes == null ? null : bytes.duplicate();

            return with(codecRegistry.codecFor(definition.type));
        }

    }
}
