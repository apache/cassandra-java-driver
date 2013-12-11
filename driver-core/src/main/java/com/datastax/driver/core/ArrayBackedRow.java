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
 * Implementation of a Row backed by an ArrayList.
 */
class ArrayBackedRow implements Row {

    private final ColumnDefinitions metadata;
    private final List<ByteBuffer> data;

    private ArrayBackedRow(ColumnDefinitions metadata, List<ByteBuffer> data) {
        this.metadata = metadata;
        this.data = data;
    }

    static Row fromData(ColumnDefinitions metadata, List<ByteBuffer> data) {
        if (data == null)
            return null;

        return new ArrayBackedRow(metadata, data);
    }

    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    public boolean isNull(int i) {
        metadata.checkBounds(i);
        return data.get(i) == null;
    }

    public boolean isNull(String name) {
        return isNull(metadata.getIdx(name));
    }

    public boolean getBool(int i) {
        metadata.checkType(i, DataType.Name.BOOLEAN);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return false;

        return BooleanType.instance.compose(value);
    }

    public boolean getBool(String name) {
        return getBool(metadata.getIdx(name));
    }

    public int getInt(int i) {
        metadata.checkType(i, DataType.Name.INT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return Int32Type.instance.compose(value);
    }

    public int getInt(String name) {
        return getInt(metadata.getIdx(name));
    }

    public long getLong(int i) {
        metadata.checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return LongType.instance.compose(value);
    }

    public long getLong(String name) {
        return getLong(metadata.getIdx(name));
    }

    public Date getDate(int i) {
        metadata.checkType(i, DataType.Name.TIMESTAMP);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DateType.instance.compose(value);
    }

    public Date getDate(String name) {
        return getDate(metadata.getIdx(name));
    }

    public float getFloat(int i) {
        metadata.checkType(i, DataType.Name.FLOAT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return FloatType.instance.compose(value);
    }

    public float getFloat(String name) {
        return getFloat(metadata.getIdx(name));
    }

    public double getDouble(int i) {
        metadata.checkType(i, DataType.Name.DOUBLE);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return DoubleType.instance.compose(value);
    }

    public double getDouble(String name) {
        return getDouble(metadata.getIdx(name));
    }

    public ByteBuffer getBytesUnsafe(int i) {
        metadata.checkBounds(i);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return value.duplicate();
    }

    public ByteBuffer getBytesUnsafe(String name) {
        return getBytesUnsafe(metadata.getIdx(name));
    }

    public ByteBuffer getBytes(int i) {
        metadata.checkType(i, DataType.Name.BLOB);
        return getBytesUnsafe(i);
    }

    public ByteBuffer getBytes(String name) {
        return getBytes(metadata.getIdx(name));
    }

    public String getString(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.VARCHAR,
                                                   DataType.Name.TEXT,
                                                   DataType.Name.ASCII);

        ByteBuffer value = data.get(i);
        if (value == null)
            return null;

        return type == DataType.Name.ASCII
             ? AsciiType.instance.compose(value)
             : UTF8Type.instance.compose(value);
    }

    public String getString(String name) {
        return getString(metadata.getIdx(name));
    }

    public BigInteger getVarint(int i) {
        metadata.checkType(i, DataType.Name.VARINT);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return IntegerType.instance.compose(value);
    }

    public BigInteger getVarint(String name) {
        return getVarint(metadata.getIdx(name));
    }

    public BigDecimal getDecimal(int i) {
        metadata.checkType(i, DataType.Name.DECIMAL);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return DecimalType.instance.compose(value);
    }

    public BigDecimal getDecimal(String name) {
        return getDecimal(metadata.getIdx(name));
    }

    public UUID getUUID(int i) {
        DataType.Name type = metadata.checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Name.UUID
             ? UUIDType.instance.compose(value)
             : TimeUUIDType.instance.compose(value);
    }

    public UUID getUUID(String name) {
        return getUUID(metadata.getIdx(name));
    }

    public InetAddress getInet(int i) {
        metadata.checkType(i, DataType.Name.INET);

        ByteBuffer value = data.get(i);
        if (value == null || value.remaining() == 0)
            return null;

        return InetAddressType.instance.compose(value);
    }

    public InetAddress getInet(String name) {
        return getInet(metadata.getIdx(name));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a list of %s (CQL type %s), cannot be retrieve as a list of %s", metadata.getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<T>emptyList();

        // TODO: we could avoid the getCodec call if we kept a reference to the original message.
        return Collections.unmodifiableList(Codec.<List<T>>getCodec(type).compose(value));
    }

    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return getList(metadata.getIdx(name), elementsClass);
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        DataType type = metadata.getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is not of set type", metadata.getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a set of %s (CQL type %s), cannot be retrieve as a set of %s", metadata.getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<T>emptySet();

        return Collections.unmodifiableSet(Codec.<Set<T>>getCodec(type).compose(value));
    }

    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return getSet(metadata.getIdx(name), elementsClass);
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

        ByteBuffer value = data.get(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return Collections.unmodifiableMap(Codec.<Map<K, V>>getCodec(type).compose(value));
    }

    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(metadata.getIdx(name), keysClass, valuesClass);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Row[");
        for (int i = 0; i < metadata.size(); i++) {
            if (i != 0)
                sb.append(", ");
            ByteBuffer bb = data.get(i);
            if (bb == null)
                sb.append("NULL");
            else
                sb.append(Codec.getCodec(metadata.getType(i)).getString(bb));
        }
        sb.append("]");
        return sb.toString();
    }
}
