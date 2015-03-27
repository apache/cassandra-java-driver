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

import com.google.common.collect.Lists;

import com.datastax.driver.core.exceptions.InvalidTypeException;

abstract class AbstractGettableData implements GettableData {

    protected final ColumnDefinitions metadata;

    protected AbstractGettableData(ColumnDefinitions metadata) {
        this.metadata = metadata;
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
        metadata.checkType(i, DataType.Name.BOOLEAN);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return false;

        return TypeCodec.BooleanCodec.instance.deserializeNoBoxing(value);
    }

    public boolean getBool(String name) {
        return getBool(metadata.getFirstIdx(name));
    }

    public int getInt(int i) {
        metadata.checkType(i, DataType.Name.INT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return TypeCodec.IntCodec.instance.deserializeNoBoxing(value);
    }

    public int getInt(String name) {
        return getInt(metadata.getFirstIdx(name));
    }

    public long getLong(int i) {
        metadata.checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return TypeCodec.LongCodec.instance.deserializeNoBoxing(value);
    }

    public long getLong(String name) {
        return getLong(metadata.getFirstIdx(name));
    }

    public Date getDate(int i) {
        metadata.checkType(i, DataType.Name.TIMESTAMP);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.DateCodec.instance.deserialize(value);
    }

    public Date getDate(String name) {
        return getDate(metadata.getFirstIdx(name));
    }

    public float getFloat(int i) {
        metadata.checkType(i, DataType.Name.FLOAT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return TypeCodec.FloatCodec.instance.deserializeNoBoxing(value);
    }

    public float getFloat(String name) {
        return getFloat(metadata.getFirstIdx(name));
    }

    public double getDouble(int i) {
        metadata.checkType(i, DataType.Name.DOUBLE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return TypeCodec.DoubleCodec.instance.deserializeNoBoxing(value);
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
        metadata.checkType(i, DataType.Name.BLOB);
        return getBytesUnsafe(i);
    }

    public ByteBuffer getBytes(String name) {
        return getBytes(metadata.getFirstIdx(name));
    }

    public String getString(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.VARCHAR,
            DataType.Name.TEXT,
            DataType.Name.ASCII);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return type == DataType.Name.ASCII
            ? TypeCodec.StringCodec.asciiInstance.deserialize(value)
            : TypeCodec.StringCodec.utf8Instance.deserialize(value);
    }

    public String getString(String name) {
        return getString(metadata.getFirstIdx(name));
    }

    public BigInteger getVarint(int i) {
        metadata.checkType(i, DataType.Name.VARINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.BigIntegerCodec.instance.deserialize(value);
    }

    public BigInteger getVarint(String name) {
        return getVarint(metadata.getFirstIdx(name));
    }

    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DataType.Name.DECIMAL);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.DecimalCodec.instance.deserialize(value);
    }

    public BigDecimal getDecimal(String name) {
        return getDecimal(metadata.getFirstIdx(name));
    }

    public UUID getUUID(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Name.UUID
            ? TypeCodec.UUIDCodec.instance.deserialize(value)
            : TypeCodec.TimeUUIDCodec.instance.deserialize(value);
    }

    public UUID getUUID(String name) {
        return getUUID(metadata.getFirstIdx(name));
    }

    public InetAddress getInet(int i) {
        metadata.checkType(i, DataType.Name.INET);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.InetCodec.instance.deserialize(value);
    }

    public InetAddress getInet(String name) {
        return getInet(metadata.getFirstIdx(name));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a list of %s (CQL type %s), cannot be retrieve as a list of %s", metadata.getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptyList();

        return Collections.unmodifiableList((List<T>)type.codec().deserialize(value));
    }

    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return getList(metadata.getFirstIdx(name), elementsClass);
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is not of set type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a set of %s (CQL type %s), cannot be retrieve as a set of %s", metadata.getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptySet();

        return Collections.unmodifiableSet((Set<T>)type.codec().deserialize(value));
    }

    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return getSet(metadata.getFirstIdx(name), elementsClass);
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is not of map type", metadata.getName(i)));

        Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
        Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
        if (!keysClass.isAssignableFrom(expectedKeysClass) || !valuesClass.isAssignableFrom(expectedValuesClass))
            throw new InvalidTypeException(String.format("Column %s is a map of %s->%s (CQL type %s), cannot be retrieve as a map of %s->%s", metadata.getName(i), expectedKeysClass, expectedValuesClass, type, keysClass, valuesClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return Collections.unmodifiableMap((Map<K, V>)type.codec().deserialize(value));
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
            return type.deserialize(raw);
    }

    public Object getObject(String name) {
        return getValue(metadata.getFirstIdx(name));
    }
}
