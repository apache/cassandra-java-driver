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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.exceptions.InvalidTypeException;

abstract class AbstractGettableByIndexData implements GettableByIndexData {

    protected final ProtocolVersion protocolVersion;

    protected AbstractGettableByIndexData(ProtocolVersion protocolVersion) {
        this.protocolVersion = protocolVersion;
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

    // Note: we avoid having a vararg method to avoid the array allocation that comes with it.
    protected void checkType(int i, DataType.Name name) {
        DataType defined = getType(i);
        if (name != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));
    }

    protected DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2) {
        DataType defined = getType(i);
        if (name1 != defined.getName() && name2 != defined.getName())
            throw new InvalidTypeException(String.format("Value %s is of type %s", getName(i), defined));

        return defined.getName();
    }

    protected DataType.Name checkType(int i, DataType.Name name1, DataType.Name name2, DataType.Name name3) {
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
    public ByteBuffer getBytes(int i) {
        checkType(i, DataType.Name.BLOB);
        return getBytesUnsafe(i);
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
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, Class<T> elementsClass) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", getName(i)));

        Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;
        if (!elementsClass.isAssignableFrom(expectedClass))
            throw new InvalidTypeException(String.format("Column %s is a list of %s (CQL type %s), cannot be retrieved as a list of %s", getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptyList();

        return Collections.unmodifiableList((List<T>)type.codec(protocolVersion).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> getList(int i, TypeToken<T> elementsType) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is not of list type", getName(i)));

        DataType expectedType = type.getTypeArguments().get(0);
        if (!expectedType.canBeDeserializedAs(elementsType))
            throw new InvalidTypeException(String.format("Column %s has CQL type %s, cannot be retrieved as a list of %s", getName(i), type, elementsType));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptyList();

        return Collections.unmodifiableList((List<T>)type.codec(protocolVersion).deserialize(value));
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
            throw new InvalidTypeException(String.format("Column %s is a set of %s (CQL type %s), cannot be retrieved as a set of %s", getName(i), expectedClass, type, elementsClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptySet();

        return Collections.unmodifiableSet((Set<T>)type.codec(protocolVersion).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(int i, TypeToken<T> elementsType) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is not of set type", getName(i)));

        DataType expectedType = type.getTypeArguments().get(0);
        if (!expectedType.canBeDeserializedAs(elementsType))
            throw new InvalidTypeException(String.format("Column %s has CQL type %s, cannot be retrieved as a set of %s", getName(i), type, elementsType));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<T>emptySet();

        return Collections.unmodifiableSet((Set<T>)type.codec(protocolVersion).deserialize(value));
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
            throw new InvalidTypeException(String.format("Column %s is a map of %s->%s (CQL type %s), cannot be retrieved as a map of %s->%s", getName(i), expectedKeysClass, expectedValuesClass, type, keysClass, valuesClass));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return Collections.unmodifiableMap((Map<K, V>)type.codec(protocolVersion).deserialize(value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(int i, TypeToken<K> keysType, TypeToken<V> valuesType) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is not of map type", getName(i)));

        DataType expectedKeysType = type.getTypeArguments().get(0);
        DataType expectedValuesType = type.getTypeArguments().get(1);
        if (!expectedKeysType.canBeDeserializedAs(keysType) || !expectedValuesType.canBeDeserializedAs(valuesType))
            throw new InvalidTypeException(String.format("Column %s has CQL type %s, cannot be retrieved as a map of %s->%s", getName(i), type, keysType, valuesType));

        ByteBuffer value = getValue(i);
        if (value == null)
            return Collections.<K, V>emptyMap();

        return Collections.unmodifiableMap((Map<K, V>)type.codec(protocolVersion).deserialize(value));
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

        return (UDTValue)type.codec(protocolVersion).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public TupleValue getTupleValue(int i) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.TUPLE)
            throw new InvalidTypeException(String.format("Column %s is not a tuple", getName(i)));

        ByteBuffer value = getValue(i);
        if (value == null || value.remaining() == 0)
            return null;

        return (TupleValue)type.codec(protocolVersion).deserialize(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getObject(int i) {
        ByteBuffer raw = getValue(i);
        DataType type = getType(i);
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
            return type.deserialize(raw, protocolVersion);
    }
}
