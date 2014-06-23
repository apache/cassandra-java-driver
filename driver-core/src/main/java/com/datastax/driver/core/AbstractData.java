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

// We don't want to expose this one: it's less useful externally and it's a bit ugly to expose anyway (but it's convenient).
abstract class AbstractData<T extends SettableData<T>> extends AbstractGettableData implements SettableData<T> {

    final T wrapped;
    final ByteBuffer[] values;

    // Ugly, we coould probably clean that: it is currently needed however because we sometimes
    // want wrapped to be 'this' (UDTValue), and sometimes some other object (in BoundStatement).
    @SuppressWarnings("unchecked")
    protected AbstractData(int protocolVersion, int size) {
        super(protocolVersion);
        this.wrapped = (T)this;
        this.values = new ByteBuffer[size];
    }

    protected AbstractData(int protocolVersion, T wrapped, int size) {
        this(protocolVersion, wrapped, new ByteBuffer[size]);
    }

    protected AbstractData(int protocolVersion, T wrapped, ByteBuffer[] values) {
        super(protocolVersion);
        this.wrapped = wrapped;
        this.values = values;
    }

    protected abstract int[] getAllIndexesOf(String name);

    private T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return wrapped;
    }

    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    protected int getIndexOf(String name) {
        return getAllIndexesOf(name)[0];
    }

    public T setBool(int i, boolean v) {
        checkType(i, DataType.Name.BOOLEAN);
        return setValue(i, TypeCodec.BooleanCodec.instance.serializeNoBoxing(v));
    }

    public T setBool(String name, boolean v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.BooleanCodec.instance.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BOOLEAN);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setInt(int i, int v) {
        checkType(i, DataType.Name.INT);
        return setValue(i, TypeCodec.IntCodec.instance.serializeNoBoxing(v));
    }

    public T setInt(String name, int v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.IntCodec.instance.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.INT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setLong(int i, long v) {
        checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);
        return setValue(i, TypeCodec.LongCodec.instance.serializeNoBoxing(v));
    }

    public T setLong(String name, long v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.LongCodec.instance.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BIGINT, DataType.Name.COUNTER);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setDate(int i, Date v) {
        checkType(i, DataType.Name.TIMESTAMP);
        return setValue(i, v == null ? null : TypeCodec.DateCodec.instance.serialize(v));
    }

    public T setDate(String name, Date v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.DateCodec.instance.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.TIMESTAMP);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setFloat(int i, float v) {
        checkType(i, DataType.Name.FLOAT);
        return setValue(i, TypeCodec.FloatCodec.instance.serializeNoBoxing(v));
    }

    public T setFloat(String name, float v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.FloatCodec.instance.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.FLOAT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setDouble(int i, double v) {
        checkType(i, DataType.Name.DOUBLE);
        return setValue(i, TypeCodec.DoubleCodec.instance.serializeNoBoxing(v));
    }

    public T setDouble(String name, double v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.DoubleCodec.instance.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.DOUBLE);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setString(int i, String v) {
        DataType.Name type = checkType(i, DataType.Name.VARCHAR, DataType.Name.TEXT, DataType.Name.ASCII);
        switch (type) {
            case ASCII:
                return setValue(i, v == null ? null : TypeCodec.StringCodec.asciiInstance.serialize(v));
            case TEXT:
            case VARCHAR:
                return setValue(i, v == null ? null : TypeCodec.StringCodec.utf8Instance.serialize(v));
            default:
                throw new AssertionError();
        }
    }

    public T setString(String name, String v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setString(indexes[i], v);
        return wrapped;
    }

    public T setBytes(int i, ByteBuffer v) {
        checkType(i, DataType.Name.BLOB);
        return setBytesUnsafe(i, v);
    }

    public T setBytes(String name, ByteBuffer v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BLOB);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    public T setBytesUnsafe(String name, ByteBuffer v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i = 0; i < indexes.length; i++)
            setValue(indexes[i], value);
        return wrapped;
    }

    public T setVarint(int i, BigInteger v) {
        checkType(i, DataType.Name.VARINT);
        return setValue(i, v == null ? null : TypeCodec.BigIntegerCodec.instance.serialize(v));
    }

    public T setVarint(String name, BigInteger v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.BigIntegerCodec.instance.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.VARINT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DataType.Name.DECIMAL);
        return setValue(i, v == null ? null : TypeCodec.DecimalCodec.instance.serialize(v));
    }

    public T setDecimal(String name, BigDecimal v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.DecimalCodec.instance.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.DECIMAL);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setUUID(int i, UUID v) {
        DataType.Name type = checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        if (v == null)
            return setValue(i, null);

        if (type == DataType.Name.TIMEUUID && v.version() != 1)
            throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));

        return type == DataType.Name.UUID
             ? setValue(i, TypeCodec.UUIDCodec.instance.serialize(v))
             : setValue(i, TypeCodec.TimeUUIDCodec.instance.serialize(v));
    }

    public T setUUID(String name, UUID v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.UUIDCodec.instance.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            DataType.Name type = checkType(indexes[i], DataType.Name.UUID, DataType.Name.TIMEUUID);
            if (v != null && type == DataType.Name.TIMEUUID && v.version() != 1)
                throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public T setInet(int i, InetAddress v) {
        checkType(i, DataType.Name.INET);
        return setValue(i, v == null ? null : TypeCodec.InetCodec.instance.serialize(v));
    }

    public T setInet(String name, InetAddress v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.InetCodec.instance.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.INET);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    public <E> T setList(int i, List<E> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.LIST)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a list", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // If the list is empty, it will never fail validation, but otherwise we should check the list given if of the right type
        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.get(0).getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).asJavaClass();

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting list of %s but provided list of %s", getName(i), type, expectedClass, providedClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public <E> T setList(String name, List<E> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setList(indexes[i], v);
        return wrapped;
    }

    public <K, V> T setMap(int i, Map<K, V> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.MAP)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a map", getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            Class<?> providedKeysClass = entry.getKey().getClass();
            Class<?> providedValuesClass = entry.getValue().getClass();

            Class<?> expectedKeysClass = type.getTypeArguments().get(0).getName().javaType;
            Class<?> expectedValuesClass = type.getTypeArguments().get(1).getName().javaType;
            if (!expectedKeysClass.isAssignableFrom(providedKeysClass) || !expectedValuesClass.isAssignableFrom(providedValuesClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting map of %s->%s but provided map of %s->%s", getName(i), type, expectedKeysClass, expectedValuesClass, providedKeysClass, providedValuesClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public <K, V> T setMap(String name, Map<K, V> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setMap(indexes[i], v);
        return wrapped;
    }

    public <E> T setSet(int i, Set<E> v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.SET)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a set", getName(i), type));

        if (v == null)
            return setValue(i, null);

        if (!v.isEmpty()) {
            // Ugly? Yes
            Class<?> providedClass = v.iterator().next().getClass();
            Class<?> expectedClass = type.getTypeArguments().get(0).getName().javaType;

            if (!expectedClass.isAssignableFrom(providedClass))
                throw new InvalidTypeException(String.format("Invalid value for column %s of CQL type %s, expecting set of %s but provided set of %s", getName(i), type, expectedClass, providedClass));
        }
        return setValue(i, type.codec(protocolVersion).serialize(v));
    }

    public <E> T setSet(String name, Set<E> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setSet(indexes[i], v);
        return wrapped;
    }

    public T setUDTValue(int i, UDTValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.UDT)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a UDT", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // UDT always user the V3 protocol version to encode values
        setValue(i, type.codec(3).serialize(v));
        return wrapped;
    }

    public T setUDTValue(String name, UDTValue v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setUDTValue(indexes[i], v);
        return wrapped;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UDTValue))
            return false;

        AbstractData<?> that = (AbstractData<?>)o;
        if (values.length != that.values.length)
            return false;

        // Deserializing each value is slightly inefficient, but comparing
        // the bytes could in theory be wrong (for varint for instance, 2 values
        // can have different binary representation but be the same value due to
        // leading zeros). So we don't take any risk.
        for (int i = 0; i < values.length; i++) {
            DataType thisType = getType(i);
            DataType thatType = that.getType(i);
            if (!thisType.equals(thatType))
                return false;

            if ((values[i] == null) != (that.values[i] == null))
                return false;

            if (values[i] != null && !(thisType.deserialize(values[i], protocolVersion).equals(thatType.deserialize(that.values[i], protocolVersion))))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash += values[i] == null ? 1 : getType(i).deserialize(values[i], protocolVersion).hashCode();
        return hash;
    }
}
