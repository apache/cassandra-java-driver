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

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.Name.*;

abstract class AbstractGettableData implements GettableData {

    protected final ColumnDefinitions metadata;

    protected final CodecRegistry codecRegistry;

    protected AbstractGettableData(ColumnDefinitions metadata, CodecRegistry codecRegistry) {
        this.metadata = metadata;
        this.codecRegistry = codecRegistry;
    }

    protected abstract ByteBuffer getValue(int i);

    public boolean isNull(int i) {
        metadata.checkBounds(i);
        return getValue(i) == null;
    }

    public boolean isNull(String name) {
        return isNull(metadata.getFirstIdx(name));
    }

    public boolean getBool(int i) {
        metadata.checkType(i, BOOLEAN);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return false;

        return codecFor(i, Boolean.class).deserialize(value);
    }

    public boolean getBool(String name) {
        return getBool(metadata.getFirstIdx(name));
    }

    public int getInt(int i) {
        metadata.checkType(i, INT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return codecFor(i, Integer.class).deserialize(value);
    }

    public int getInt(String name) {
        return getInt(metadata.getFirstIdx(name));
    }

    public long getLong(int i) {
        metadata.checkType(i, BIGINT, COUNTER);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return codecFor(i, Long.class).deserialize(value);
    }

    public long getLong(String name) {
        return getLong(metadata.getFirstIdx(name));
    }

    public Date getDate(int i) {
        metadata.checkType(i, TIMESTAMP);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, Date.class).deserialize(value);
    }

    public Date getDate(String name) {
        return getDate(metadata.getFirstIdx(name));
    }

    public float getFloat(int i) {
        metadata.checkType(i, FLOAT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return codecFor(i, Float.class).deserialize(value);
    }

    public float getFloat(String name) {
        return getFloat(metadata.getFirstIdx(name));
    }

    public double getDouble(int i) {
        metadata.checkType(i, DOUBLE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return codecFor(i, Double.class).deserialize(value);
    }

    public double getDouble(String name) {
        return getDouble(metadata.getFirstIdx(name));
    }

    public ByteBuffer getBytesUnsafe(int i) {
        metadata.checkBounds(i);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return value.duplicate();
    }

    public ByteBuffer getBytesUnsafe(String name) {
        return getBytesUnsafe(metadata.getFirstIdx(name));
    }

    public ByteBuffer getBytes(int i) {
        metadata.checkType(i, BLOB);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return codecFor(i, ByteBuffer.class).deserialize(value);
    }

    public ByteBuffer getBytes(String name) {
        return getBytes(metadata.getFirstIdx(name));
    }

    public String getString(int i) {
        metadata.checkType(i, VARCHAR, TEXT, ASCII);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return codecFor(i, String.class).deserialize(value);
    }

    public String getString(String name) {
        return getString(metadata.getFirstIdx(name));
    }

    public BigInteger getVarint(int i) {
        metadata.checkType(i, VARINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, BigInteger.class).deserialize(value);
    }

    public BigInteger getVarint(String name) {
        return getVarint(metadata.getFirstIdx(name));
    }

    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DECIMAL);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, BigDecimal.class).deserialize(value);
    }

    public BigDecimal getDecimal(String name) {
        return getDecimal(metadata.getFirstIdx(name));
    }

    public UUID getUUID(int i) {
        metadata.checkType(i, UUID, TIMEUUID);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, UUID.class).deserialize(value);
    }

    public UUID getUUID(String name) {
        return getUUID(metadata.getFirstIdx(name));
    }

    public InetAddress getInet(int i) {
        metadata.checkType(i, INET);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return codecFor(i, InetAddress.class).deserialize(value);
    }

    public InetAddress getInet(String name) {
        return getInet(metadata.getFirstIdx(name));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        metadata.checkType(i, LIST);

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.emptyList();

        TypeToken<List<T>> javaType = listOf(elementsClass);
        List<T> list = codecFor(i, javaType).deserialize(value);
        return Collections.unmodifiableList(list);
    }

    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return getList(metadata.getFirstIdx(name), elementsClass);
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        metadata.checkType(i, SET);

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.emptySet();

        TypeToken<Set<T>> javaType = setOf(elementsClass);
        Set<T> set = codecFor(i, javaType).deserialize(value);
        return Collections.unmodifiableSet(set);
    }

    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return getSet(metadata.getFirstIdx(name), elementsClass);
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        metadata.checkType(i, MAP);

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.emptyMap();

        TypeToken<Map<K, V>> javaType = mapOf(keysClass, valuesClass);
        Map<K, V> map = codecFor(i, javaType).deserialize(value);
        return Collections.unmodifiableMap(map);
    }

    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(metadata.getFirstIdx(name), keysClass, valuesClass);
    }

    public Object getObject(int i) {
        ByteBuffer raw = getValue(i);
        DataType type = metadata.getType(i);
        if (raw == null)
            switch (type.getName()) {
                case LIST:
                    return Collections.emptyList();
                case SET:
                    return Collections.emptySet();
                case MAP:
                    return Collections.emptyMap();
                default:
                    return null;
            }
        else
            return codecFor(i).deserialize(raw);
    }

    public Object getObject(String name) {
        return getObject(metadata.getFirstIdx(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(int i, Class<T> targetClass) {
       return getObject(i, TypeToken.of(targetClass));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(int i, TypeToken<T> targetType) {
        ByteBuffer raw = getValue(i);
        if(raw == null)
            return null;
        TypeCodec<T> codec = codecFor(i, targetType);
        return codec.deserialize(raw);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(String name, Class<T> targetClass) {
        return getObject(metadata.findFirstIdx(name), targetClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getObject(String name, TypeToken<T> targetType) {
        return getObject(metadata.findFirstIdx(name), targetType);
    }

    protected CodecRegistry getCodecRegistry() {
        return codecRegistry;
    }

    protected <T> TypeCodec<T> codecFor(int i){
        return getCodecRegistry().codecFor(metadata.getType(i), metadata.getKeyspace(i), metadata.getTable(i), metadata.getName(i));
    }

    protected <T> TypeCodec<T> codecFor(int i, T value){
        return getCodecRegistry().codecFor(metadata.getType(i), value, metadata.getKeyspace(i), metadata.getTable(i), metadata.getName(i));
    }

    protected <T> TypeCodec<T> codecFor(int i, Class<T> javaClass){
        return codecFor(i, TypeToken.of(javaClass));
    }

    protected <T> TypeCodec<T> codecFor(int i, TypeToken<T> javaType){
        return getCodecRegistry().codecFor(metadata.getType(i), javaType, metadata.getKeyspace(i), metadata.getTable(i), metadata.getName(i));
    }
}
