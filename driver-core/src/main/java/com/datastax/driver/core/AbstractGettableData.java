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

import com.datastax.driver.core.exceptions.InvalidTypeException;

public abstract class AbstractGettableData implements GettableData {

    final int version;

    /**
     * Creates a new AbstractGettableData object.
     *
     * @param protocolVersion the protocol version in which values returned
     * by {@link #getValue} will be returned. This must be a protocol version
     * supported by this driver. In general, the correct value will be the
     * value returned by {@link ProtocolOptions#getProtocolVersion}.
     *
     * @throws IllegalArgumentException if {@code protocolVersion} is not a valid protocol version.
     */
    protected AbstractGettableData(int protocolVersion) {
        if (protocolVersion == 0 || protocolVersion > ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION)
            throw new IllegalArgumentException(String.format("Unsupported protocol version %d; valid values must be between 1 and %d or negative (for auto-detect).",
                                                             protocolVersion,
                                                             ProtocolOptions.NEWEST_SUPPORTED_PROTOCOL_VERSION));
        this.version = protocolVersion;
    }

    /**
     * Returns the type for the value at index {@code i}.
     *
     * @param i the index of the type to fetch.
     * @return the type of the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract DataType getType(int i);

    /**
     * Returns the name corresponding to the value at index {@code i}.
     *
     * @param i the index of the name to fetch.
     * @return the name corresponding to the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract String getName(int i);

    /**
     * Returns the value at index {@code i}.
     *
     * @param i the index to fetch.
     * @return the value at index {@code i}.
     *
     * @throws IndexOutOfBoundsException if {@code i} is not a valid index.
     */
    protected abstract ByteBuffer getValue(int i);

    /**
     * Returns the index corresponding to a given name.
     *
     * @param name the name for which to return the index of.
     * @return the index for the value coressponding to {@code name}.
     *
     * @throws IllegalArgumentException if {@code name} is not valid name for this object.
     */
    protected abstract int getIndexOf(String name);

    // Note: we avoid having a vararg method to avoid the array allocation that comes with it.
    void checkType(int i, DataType.Name name) {
        DataType defined = getType(i);
        if (name != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));
    }

    DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));

        return defined.getName();
    }

    DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2, DataType.Name name3) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName() && name3 != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));

        return defined.getName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(int i) {
        return getValue(i) == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isNull(String name) {
        return isNull(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(int i) {
        checkType(i, DataType.Name.BOOLEAN);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return false;

        return TypeCodec.BooleanCodec.instance.deserializeNoBoxing(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getBool(String name) {
        return getBool(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(int i) {
        checkType(i, DataType.Name.INT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0;

        return TypeCodec.IntCodec.instance.deserializeNoBoxing(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInt(String name) {
        return getInt(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(int i) {
        checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0L;

        return TypeCodec.LongCodec.instance.deserializeNoBoxing(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLong(String name) {
        return getLong(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(int i) {
        checkType(i, DataType.Name.TIMESTAMP);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.DateCodec.instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getDate(String name) {
        return getDate(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(int i) {
        checkType(i, DataType.Name.FLOAT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0f;

        return TypeCodec.FloatCodec.instance.deserializeNoBoxing(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public float getFloat(String name) {
        return getFloat(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(int i) {
        checkType(i, DataType.Name.DOUBLE);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return 0.0;

        return TypeCodec.DoubleCodec.instance.deserializeNoBoxing(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public double getDouble(String name) {
        return getDouble(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(int i) {
        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return value.duplicate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytesUnsafe(String name) {
        return getBytesUnsafe(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(int i) {
        checkType(i, DataType.Name.BLOB);
        return getBytesUnsafe(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ByteBuffer getBytes(String name) {
        return getBytes(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(int i) {
        DataType.Name type = checkType(i, DataType.Name.VARCHAR,
                                          DataType.Name.TEXT,
                                          DataType.Name.ASCII);

        ByteBuffer value = getValue(i);
        if (value == null)
            return null;

        return type == DataType.Name.ASCII
             ? TypeCodec.StringCodec.asciiInstance.deserialize(value)
             : TypeCodec.StringCodec.utf8Instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getString(String name) {
        return getString(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(int i) {
        checkType(i, DataType.Name.VARINT);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.BigIntegerCodec.instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getVarint(String name) {
        return getVarint(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(int i) {
        checkType(i, DataType.Name.DECIMAL);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.DecimalCodec.instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigDecimal getDecimal(String name) {
        return getDecimal(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(int i) {
        DataType.Name type = checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return type == DataType.Name.UUID
             ? TypeCodec.UUIDCodec.instance.deserialize(value)
             : TypeCodec.TimeUUIDCodec.instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getUUID(String name) {
        return getUUID(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(int i) {
        checkType(i, DataType.Name.INET);

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return TypeCodec.InetCodec.instance.deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetAddress getInet(String name) {
        return getInet(getIndexOf(name));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a list of %s (CQL type %s), cannot be retrieve as a list of %s", getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptyList();

        return Collections.unmodifiableList((List<T>)type.codec(version).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> getList(String name, Class<T> elementsClass) {
        return getList(getIndexOf(name), elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, Class<T> elementsClass) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is not of set type", getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a set of %s (CQL type %s), cannot be retrieve as a set of %s", getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptySet();

        return Collections.unmodifiableSet((Set<T>)type.codec(version).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Set<T> getSet(String name, Class<T> elementsClass) {
        return getSet(getIndexOf(name), elementsClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, Class<K> keysClass, Class<V> valuesClass) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is not of map type", getName(i)));

        Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
        Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
        if (!keysClass.isAssignableFrom(expectedKeysClass) || !valuesClass.isAssignableFrom(expectedValuesClass))
            throw new InvalidTypeException(String.format("Column %s is a map of %s->%s (CQL type %s), cannot be retrieve as a map of %s->%s", getName(i), expectedKeysClass, expectedValuesClass, type, keysClass, valuesClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return Collections.unmodifiableMap((Map<K, V>)type.codec(version).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K, V> Map<K, V> getMap(String name, Class<K> keysClass, Class<V> valuesClass) {
        return getMap(getIndexOf(name), keysClass, valuesClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public UDTValue getUDTValue(int i) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.UDT)
            throw new InvalidTypeException(String.format("Column %s is not a UDT", getName(i)));

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        // UDT always use the protocol V3 to encode values
        return (UDTValue)type.codec(3).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UDTValue getUDTValue(String name) {
        return getUDTValue(getIndexOf(name));
    }
}
