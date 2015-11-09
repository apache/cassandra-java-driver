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
package com.datastax.driver.core.querybuilder;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Pattern;

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Common ancestor to the query builder built statements.
 */
public abstract class BuiltStatement extends RegularStatement {

    private static final Pattern lowercaseId = Pattern.compile("[a-z][a-z0-9_]*");

    private final List<ColumnMetadata> partitionKey;
    private final List<Object> routingKeyValues;
    final String keyspace;

    private boolean dirty;
    private String cache;
    Boolean isCounterOp;
    boolean hasNonIdempotentOps;

    // Whether the user has input bind markers. If that's the case, we never generate values as
    // it means the user meant for the statement to be prepared and we shouldn't add our own markers.
    // In practice, setting this flag to true will cause all the values to be formatted as CQL literals
    // in the query.
    boolean hasBindMarkers;
    private boolean forceNoValues;

    BuiltStatement(String keyspace, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        super(protocolVersion, codecRegistry);
        this.partitionKey = null;
        this.routingKeyValues = null;
        this.keyspace = keyspace;
    }

    BuiltStatement(TableMetadata tableMetadata, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        super(protocolVersion, codecRegistry);
        this.partitionKey = tableMetadata.getPartitionKey();
        this.routingKeyValues = Arrays.asList(new Object[tableMetadata.getPartitionKey().size()]);
        this.keyspace = escapeId(tableMetadata.getKeyspace().getName());
    }

    // Same as Metadata.escapeId, but we don't have access to it here.
    protected static String escapeId(String ident) {
        // we don't need to escape if it's lowercase and match non-quoted CQL3 ids.
        return lowercaseId.matcher(ident).matches() ? ident : Metadata.quote(ident);
    }

    @Override
    public String getQueryString() {
        maybeRebuildCache();
        return cache;
    }

    private void maybeRebuildCache() {
        if (!dirty && cache != null)
            return;

        StringBuilder sb;

        if (hasBindMarkers || forceNoValues) {
            bind(); // clear any existing values
            sb = buildQueryString(null);
        } else {
            List<Object> values = new ArrayList<Object>();
            sb = buildQueryString(values);

            if (values.size() > 65535)
                throw new IllegalArgumentException("Too many values for built statement, the maximum allowed is 65535");

            bind(values.toArray());
        }

        maybeAddSemicolon(sb);

        cache = sb.toString();
        dirty = false;
    }

    static StringBuilder maybeAddSemicolon(StringBuilder sb) {
        // Use the same test that String#trim() uses to determine
        // if a character is a whitespace character.
        int l = sb.length();
        while (l > 0 && sb.charAt(l - 1) <= ' ')
            l -= 1;
        if (l != sb.length())
            sb.setLength(l);

        if (l == 0 || sb.charAt(l - 1) != ';')
            sb.append(';');
        return sb;
    }

    abstract StringBuilder buildQueryString(List<Object> variables);

    boolean isCounterOp() {
        return isCounterOp == null ? false : isCounterOp;
    }

    void setCounterOp(boolean isCounterOp) {
        this.isCounterOp = isCounterOp;
    }

    boolean hasNonIdempotentOps() {
        return hasNonIdempotentOps;
    }

    void setNonIdempotentOps() {
        hasNonIdempotentOps = true;
    }

    void checkForBindMarkers(Object value) {
        dirty = true;
        if (Utils.containsBindMarker(value))
            hasBindMarkers = true;
    }

    void checkForBindMarkers(Utils.Appendeable value) {
        dirty = true;
        if (value != null && value.containsBindMarker())
            hasBindMarkers = true;
    }

    // TODO: Correctly document the InvalidTypeException
    void maybeAddRoutingKey(String name, Object value) {
        if (routingKeyValues == null || name == null || value == null || Utils.containsSpecialValue(value))
            return;

        for (int i = 0; i < partitionKey.size(); i++) {
            if (name.equals(partitionKey.get(i).getName())) {
                routingKeyValues.set(i, value);
                return;
            }
        }
    }

    @Override
    public ByteBuffer getRoutingKey() {
        if (routingKeyValues == null)
            return null;
        ByteBuffer[] routingKeyParts = new ByteBuffer[partitionKey.size()];
        for (int i = 0; i < partitionKey.size(); i++) {
            Object value = routingKeyValues.get(i);
            if(value == null)
                return null;
            TypeCodec<Object> codec = codecRegistry.codecFor(partitionKey.get(i).getType(), value);
            routingKeyParts[i] = codec.serialize(value, protocolVersion);
        }
        return routingKeyParts.length == 1
            ? routingKeyParts[0]
            : Utils.compose(routingKeyParts);
    }

    @Override
    public String getKeyspace() {
        return keyspace;
    }

    @Override
    protected void maybeRefreshValues() {
        maybeRebuildCache();
    }

    @Override
    public Boolean isIdempotent() {
        // If a value was forced with setIdempotent, it takes priority
        if (idempotent != null)
            return idempotent;

        // Otherwise return the computed value
        return !hasNonIdempotentOps();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Note: Calling this method may trigger the underlying {@link Cluster} initialization.
     */
    @Override
    public String toString() {
        if (forceNoValues) {
            return getQueryString();
        }
        return maybeAddSemicolon(buildQueryString(null)).toString();
    }

    /**
     * Allows to force this builder to not generate values (through its {@code getValues()} method).
     * <p>
     * By default (and unless the protocol version 1 is in use, see below) and
     * for performance reasons, the query builder will not serialize all values
     * provided to strings. This means that the {@link #getQueryString} may
     * return a query string with bind markers (where and when is at the
     * discretion of the builder) and {@link RegularStatement#getValueDefinitions() values}
     * will be set for those markers. This method allows to force the builder to not
     * generate binary values but rather to serialize them all in the query
     * string. In practice, this means that if you call {@code
     * setForceNoValues(true)}, you are guaranteed that {@code getValues()} will
     * return {@code null} and that the string returned by {@code
     * getQueryString()} will contain no other bind markers than the one
     * inputted by the user.
     * <p>
     * If the native protocol version 1 is in use, the driver will default
     * to not generating values since those are not supported by that version of
     * the protocol. In practice, the driver will automatically call this method
     * with {@code true} as argument prior to execution. Hence, calling this
     * method when the protocol version 1 is in use is basically a no-op.
     * <p>
     * Note that this method is mainly useful for debugging purpose. In general,
     * the default behavior should be the correct and most efficient one.
     *
     * @param forceNoValues whether or not this builder may generate values.
     * @return this statement.
     */
    public RegularStatement setForceNoValues(boolean forceNoValues) {
        this.forceNoValues = forceNoValues;
        this.dirty = true;
        return this;
    }

    /**
     * A utility class to create a BuiltStatement that encapsulates another one.
     */
    abstract static class ForwardingStatement<T extends BuiltStatement> extends BuiltStatement {

        T statement;

        ForwardingStatement(T statement) {
            super((String)null, statement.protocolVersion, statement.codecRegistry);
            this.statement = statement;
        }

        @Override
        public String getQueryString() {
            return statement.getQueryString();
        }

        @Override
        StringBuilder buildQueryString(List<Object> values) {
            return statement.buildQueryString(values);
        }

        @Override
        public ByteBuffer getRoutingKey() {
            return statement.getRoutingKey();
        }

        @Override
        public String getKeyspace() {
            return statement.getKeyspace();
        }

        @Override
        boolean isCounterOp() {
            return statement.isCounterOp();
        }

        @Override
        boolean hasNonIdempotentOps() {
            return statement.hasNonIdempotentOps();
        }

        @Override
        public RegularStatement setForceNoValues(boolean forceNoValues) {
            statement.setForceNoValues(forceNoValues);
            return this;
        }

        @Override
        public Statement setConsistencyLevel(ConsistencyLevel consistency) {
            statement.setConsistencyLevel(consistency);
            return this;
        }

        @Override
        public ConsistencyLevel getConsistencyLevel() {
            return statement.getConsistencyLevel();
        }

        @Override
        public Statement enableTracing() {
            statement.enableTracing();
            return this;
        }

        @Override
        public Statement disableTracing() {
            statement.disableTracing();
            return this;
        }

        @Override
        public boolean isTracing() {
            return statement.isTracing();
        }

        @Override
        public Statement setRetryPolicy(RetryPolicy policy) {
            statement.setRetryPolicy(policy);
            return this;
        }

        @Override
        public RetryPolicy getRetryPolicy() {
            return statement.getRetryPolicy();
        }

        @Override
        public boolean hasValues() {
            return statement.hasValues();
        }

        @Override
        public boolean usesPositionalValues() {
            return statement.usesPositionalValues();
        }

        @Override
        public boolean usesNamedValues() {
            return statement.usesNamedValues();
        }

        @Override
        public List<ValueDefinition> getValueDefinitions() {
            return statement.getValueDefinitions();
        }

        @Override
        public int valuesCount() {
            return statement.valuesCount();
        }

        @Override
        protected List<ByteBuffer> getValues() {
            return statement.getValues();
        }

        @Override
        protected List<String> getValueNames() {
            return statement.getValueNames();
        }

        @Override
        public RegularStatement bind(Object... values) {
            return statement.bind(values);
        }

        @Override
        public boolean isSet(int i) {
            return statement.isSet(i);
        }

        @Override
        public boolean isSet(String name) {
            return statement.isSet(name);
        }

        @Override
        public void unset(int i) {
            statement.unset(i);
        }

        @Override
        public void unset(String name) {
            statement.unset(name);
        }

        @Override
        public RegularStatement setToNull(int i) {
            return statement.setToNull(i);
        }

        @Override
        public RegularStatement setToNull(String name) {
            return statement.setToNull(name);
        }

        @Override
        public boolean isNull(int i) {
            return statement.isNull(i);
        }

        @Override
        public boolean isNull(String name) {
            return statement.isNull(name);
        }

        @Override
        public ByteBuffer getBytesUnsafe(int i) {
            return statement.getBytesUnsafe(i);
        }

        @Override
        public ByteBuffer getBytesUnsafe(String name) {
            return statement.getBytesUnsafe(name);
        }

        @Override
        public RegularStatement setBytesUnsafe(int i, ByteBuffer v) {
            return statement.setBytesUnsafe(i, v);
        }

        @Override
        public RegularStatement setBytesUnsafe(String name, ByteBuffer v) {
            return statement.setBytesUnsafe(name, v);
        }

        @Override
        public ByteBuffer getBytes(int i) {
            return statement.getBytes(i);
        }

        @Override
        public ByteBuffer getBytes(String name) {
            return statement.getBytes(name);
        }

        @Override
        public RegularStatement setBytes(int i, ByteBuffer v) {
            return statement.setBytes(i, v);
        }

        @Override
        public RegularStatement setBytes(String name, ByteBuffer v) {
            return statement.setBytes(name, v);
        }

        @Override
        public boolean getBool(int i) {
            return statement.getBool(i);
        }

        @Override
        public boolean getBool(String name) {
            return statement.getBool(name);
        }

        @Override
        public RegularStatement setBool(int i, boolean v) {
            return statement.setBool(i, v);
        }

        @Override
        public RegularStatement setBool(String name, boolean v) {
            return statement.setBool(name, v);
        }

        @Override
        public byte getByte(int i) {
            return statement.getByte(i);
        }

        @Override
        public byte getByte(String name) {
            return statement.getByte(name);
        }

        @Override
        public RegularStatement setByte(int i, byte v) {
            return statement.setByte(i, v);
        }

        @Override
        public RegularStatement setByte(String name, byte v) {
            return statement.setByte(name, v);
        }

        @Override
        public short getShort(int i) {
            return statement.getShort(i);
        }

        @Override
        public short getShort(String name) {
            return statement.getShort(name);
        }

        @Override
        public RegularStatement setShort(int i, short v) {
            return statement.setShort(i, v);
        }

        @Override
        public RegularStatement setShort(String name, short v) {
            return statement.setShort(name, v);
        }

        @Override
        public int getInt(int i) {
            return statement.getInt(i);
        }

        @Override
        public int getInt(String name) {
            return statement.getInt(name);
        }

        @Override
        public RegularStatement setInt(int i, int v) {
            return statement.setInt(i, v);
        }

        @Override
        public RegularStatement setInt(String name, int v) {
            return statement.setInt(name, v);
        }

        @Override
        public long getLong(int i) {
            return statement.getLong(i);
        }

        @Override
        public long getLong(String name) {
            return statement.getLong(name);
        }

        @Override
        public RegularStatement setLong(int i, long v) {
            return statement.setLong(i, v);
        }

        @Override
        public RegularStatement setLong(String name, long v) {
            return statement.setLong(name, v);
        }

        @Override
        public float getFloat(int i) {
            return statement.getFloat(i);
        }

        @Override
        public float getFloat(String name) {
            return statement.getFloat(name);
        }

        @Override
        public RegularStatement setFloat(int i, float v) {
            return statement.setFloat(i, v);
        }

        @Override
        public RegularStatement setFloat(String name, float v) {
            return statement.setFloat(name, v);
        }

        @Override
        public double getDouble(int i) {
            return statement.getDouble(i);
        }

        @Override
        public double getDouble(String name) {
            return statement.getDouble(name);
        }

        @Override
        public RegularStatement setDouble(int i, double v) {
            return statement.setDouble(i, v);
        }

        @Override
        public RegularStatement setDouble(String name, double v) {
            return statement.setDouble(name, v);
        }

        @Override
        public BigInteger getVarint(int i) {
            return statement.getVarint(i);
        }

        @Override
        public BigInteger getVarint(String name) {
            return statement.getVarint(name);
        }

        @Override
        public RegularStatement setVarint(int i, BigInteger v) {
            return statement.setVarint(i, v);
        }

        @Override
        public RegularStatement setVarint(String name, BigInteger v) {
            return statement.setVarint(name, v);
        }

        @Override
        public BigDecimal getDecimal(int i) {
            return statement.getDecimal(i);
        }

        @Override
        public BigDecimal getDecimal(String name) {
            return statement.getDecimal(name);
        }

        @Override
        public RegularStatement setDecimal(int i, BigDecimal v) {
            return statement.setDecimal(i, v);
        }

        @Override
        public RegularStatement setDecimal(String name, BigDecimal v) {
            return statement.setDecimal(name, v);
        }

        @Override
        public String getString(int i) {
            return statement.getString(i);
        }

        @Override
        public String getString(String name) {
            return statement.getString(name);
        }

        @Override
        public RegularStatement setString(int i, String v) {
            return statement.setString(i, v);
        }

        @Override
        public RegularStatement setString(String name, String v) {
            return statement.setString(name, v);
        }

        @Override
        public Date getTimestamp(int i) {
            return statement.getTimestamp(i);
        }

        @Override
        public Date getTimestamp(String name) {
            return statement.getTimestamp(name);
        }

        @Override
        public RegularStatement setTimestamp(int i, Date v) {
            return statement.setTimestamp(i, v);
        }

        @Override
        public RegularStatement setTimestamp(String name, Date v) {
            return statement.setTimestamp(name, v);
        }

        @Override
        public long getTime(int i) {
            return statement.getTime(i);
        }

        @Override
        public long getTime(String name) {
            return statement.getTime(name);
        }

        @Override
        public RegularStatement setTime(int i, long v) {
            return statement.setTime(i, v);
        }

        @Override
        public RegularStatement setTime(String name, long v) {
            return statement.setTime(name, v);
        }

        @Override
        public LocalDate getDate(int i) {
            return statement.getDate(i);
        }

        @Override
        public LocalDate getDate(String name) {
            return statement.getDate(name);
        }

        @Override
        public RegularStatement setDate(int i, LocalDate v) {
            return statement.setDate(i, v);
        }

        @Override
        public RegularStatement setDate(String name, LocalDate v) {
            return statement.setDate(name, v);
        }

        @Override
        public UUID getUUID(int i) {
            return statement.getUUID(i);
        }

        @Override
        public UUID getUUID(String name) {
            return statement.getUUID(name);
        }

        @Override
        public RegularStatement setUUID(int i, UUID v) {
            return statement.setUUID(i, v);
        }

        @Override
        public RegularStatement setUUID(String name, UUID v) {
            return statement.setUUID(name, v);
        }

        @Override
        public InetAddress getInet(int i) {
            return statement.getInet(i);
        }

        @Override
        public InetAddress getInet(String name) {
            return statement.getInet(name);
        }

        @Override
        public RegularStatement setInet(int i, InetAddress v) {
            return statement.setInet(i, v);
        }

        @Override
        public RegularStatement setInet(String name, InetAddress v) {
            return statement.setInet(name, v);
        }

        @Override
        public UDTValue getUDTValue(int i) {
            return statement.getUDTValue(i);
        }

        @Override
        public UDTValue getUDTValue(String name) {
            return statement.getUDTValue(name);
        }

        @Override
        public RegularStatement setUDTValue(int i, UDTValue v) {
            return statement.setUDTValue(i, v);
        }

        @Override
        public RegularStatement setUDTValue(String name, UDTValue v) {
            return statement.setUDTValue(name, v);
        }

        @Override
        public TupleValue getTupleValue(int i) {
            return statement.getTupleValue(i);
        }

        @Override
        public TupleValue getTupleValue(String name) {
            return statement.getTupleValue(name);
        }

        @Override
        public RegularStatement setTupleValue(int i, TupleValue v) {
            return statement.setTupleValue(i, v);
        }

        @Override
        public RegularStatement setTupleValue(String name, TupleValue v) {
            return statement.setTupleValue(name, v);
        }

        @Override
        public <V> List<V> getList(int i, Class<V> elementsClass) {
            return statement.getList(i, elementsClass);
        }

        @Override
        public <V> List<V> getList(String name, Class<V> elementsClass) {
            return statement.getList(name, elementsClass);
        }

        @Override
        public <V> List<V> getList(int i, TypeToken<V> elementsType) {
            return statement.getList(i, elementsType);
        }

        @Override
        public <V> List<V> getList(String name, TypeToken<V> elementsType) {
            return statement.getList(name, elementsType);
        }

        @Override
        public <E> RegularStatement setList(int i, List<E> v) {
            return statement.setList(i, v);
        }

        @Override
        public <E> RegularStatement setList(String name, List<E> v) {
            return statement.setList(name, v);
        }

        @Override
        public <E> RegularStatement setList(int i, List<E> v, Class<E> elementsClass) {
            return statement.setList(i, v, elementsClass);
        }

        @Override
        public <E> RegularStatement setList(String name, List<E> v, Class<E> elementsClass) {
            return statement.setList(name, v, elementsClass);
        }

        @Override
        public <E> RegularStatement setList(int i, List<E> v, TypeToken<E> elementsType) {
            return statement.setList(i, v, elementsType);
        }

        @Override
        public <E> RegularStatement setList(String name, List<E> v, TypeToken<E> elementsType) {
            return statement.setList(name, v, elementsType);
        }

        @Override
        public <V> Set<V> getSet(int i, Class<V> elementsClass) {
            return statement.getSet(i, elementsClass);
        }

        @Override
        public <V> Set<V> getSet(String name, Class<V> elementsClass) {
            return statement.getSet(name, elementsClass);
        }

        @Override
        public <V> Set<V> getSet(int i, TypeToken<V> elementsType) {
            return statement.getSet(i, elementsType);
        }

        @Override
        public <V> Set<V> getSet(String name, TypeToken<V> elementsType) {
            return statement.getSet(name, elementsType);
        }

        @Override
        public <E> RegularStatement setSet(int i, Set<E> v) {
            return statement.setSet(i, v);
        }

        @Override
        public <E> RegularStatement setSet(String name, Set<E> v) {
            return statement.setSet(name, v);
        }

        @Override
        public <E> RegularStatement setSet(int i, Set<E> v, Class<E> elementsClass) {
            return statement.setSet(i, v, elementsClass);
        }

        @Override
        public <E> RegularStatement setSet(String name, Set<E> v, Class<E> elementsClass) {
            return statement.setSet(name, v, elementsClass);
        }

        @Override
        public <E> RegularStatement setSet(int i, Set<E> v, TypeToken<E> elementsType) {
            return statement.setSet(i, v, elementsType);
        }

        @Override
        public <E> RegularStatement setSet(String name, Set<E> v, TypeToken<E> elementsType) {
            return statement.setSet(name, v, elementsType);
        }

        @Override
        public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
            return statement.getMap(i, keysClass, valuesClass);
        }

        @Override
        public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
            return statement.getMap(name, keysClass, valuesClass);
        }

        @Override
        public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
            return statement.getMap(i, keysType, valuesType);
        }

        @Override
        public <K, V> Map<K, V> getMap(String name, TypeToken<K> keysType, TypeToken<V> valuesType) {
            return statement.getMap(name, keysType, valuesType);
        }

        @Override
        public <K, V> RegularStatement setMap(int i, Map<K, V> v) {
            return statement.setMap(i, v);
        }

        @Override
        public <K, V> RegularStatement setMap(String name, Map<K, V> v) {
            return statement.setMap(name, v);
        }

        @Override
        public <K, V> RegularStatement setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
            return statement.setMap(i, v, keysClass, valuesClass);
        }

        @Override
        public <K, V> RegularStatement setMap(String name, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
            return statement.setMap(name, v, keysClass, valuesClass);
        }

        @Override
        public <K, V> RegularStatement setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
            return statement.setMap(i, v, keysType, valuesType);
        }

        @Override
        public <K, V> RegularStatement setMap(String name, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
            return statement.setMap(name, v, keysType, valuesType);
        }

        @Override
        public Object getObject(int i) {
            return statement.getObject(i);
        }

        @Override
        public Object getObject(String name) {
            return statement.getObject(name);
        }

        @Override
        public <V> V get(int i, Class<V> targetClass) {
            return statement.get(i, targetClass);
        }

        @Override
        public <V> V get(String name, Class<V> targetClass) {
            return statement.get(name, targetClass);
        }

        @Override
        public <V> V get(int i, TypeToken<V> targetType) {
            return statement.get(i, targetType);
        }

        @Override
        public <V> V get(String name, TypeToken<V> targetType) {
            return statement.get(name, targetType);
        }

        @Override
        public <V> V get(int i, TypeCodec<V> codec) {
            return statement.get(i, codec);
        }

        @Override
        public <V> V get(String name, TypeCodec<V> codec) {
            return statement.get(name, codec);
        }

        @Override
        public <V> RegularStatement set(int i, V v, Class<V> targetClass) {
            return statement.set(i, v, targetClass);
        }

        @Override
        public <V> RegularStatement set(String name, V v, Class<V> targetClass) {
            return statement.set(name, v, targetClass);
        }

        @Override
        public <V> RegularStatement set(int i, V v, TypeToken<V> targetType) {
            return statement.set(i, v, targetType);
        }

        @Override
        public <V> RegularStatement set(String name, V v, TypeToken<V> targetType) {
            return statement.set(name, v, targetType);
        }

        @Override
        public <V> RegularStatement set(int i, V v, TypeCodec<V> codec) {
            return statement.set(i, v, codec);
        }

        @Override
        public <V> RegularStatement set(String name, V v, TypeCodec<V> codec) {
            return statement.set(name, v, codec);
        }

        @Override
        public RegularStatement setToken(int i, Token v) {
            return statement.setToken(i, v);
        }

        @Override
        public RegularStatement setToken(String name, Token v) {
            return statement.setToken(name, v);
        }

        @Override
        public RegularStatement setPartitionKeyToken(Token v) {
            return statement.setPartitionKeyToken(v);
        }

        @Override
        void checkForBindMarkers(Object value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        void checkForBindMarkers(Utils.Appendeable value) {
            statement.checkForBindMarkers(value);
        }

        @Override
        public String toString() {
            return statement.toString();
        }
    }
}
