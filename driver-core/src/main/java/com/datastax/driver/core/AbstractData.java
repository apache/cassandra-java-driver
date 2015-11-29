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

import com.datastax.driver.core.exceptions.InvalidTypeException;

// We don't want to expose this one: it's less useful externally and it's a bit ugly to expose anyway (but it's convenient).
abstract class AbstractData<T extends SettableData<T>> extends AbstractGettableData implements SettableData<T> {

    final T wrapped;
    final ByteBuffer[] values;

    // Ugly, we coould probably clean that: it is currently needed however because we sometimes
    // want wrapped to be 'this' (UDTValue), and sometimes some other object (in BoundStatement).
    @SuppressWarnings("unchecked")
    protected AbstractData(ProtocolVersion protocolVersion, int size) {
        super(protocolVersion);
        this.wrapped = (T)this;
        this.values = new ByteBuffer[size];
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, int size) {
        this(protocolVersion, wrapped, new ByteBuffer[size]);
    }

    protected AbstractData(ProtocolVersion protocolVersion, T wrapped, ByteBuffer[] values) {
        super(protocolVersion);
        this.wrapped = wrapped;
        this.values = values;
    }

    protected abstract int[] getAllIndexesOf(String name);

    private T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return wrapped;
    }

    @Override
    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    @Override
    protected int getIndexOf(String name) {
        return getAllIndexesOf(name)[0];
    }

    @Override
    public T setBool(int i, boolean v) {
        checkType(i, DataType.Name.BOOLEAN);
        return setValue(i, TypeCodec.booleanCodec.serializeNoBoxing(v));
    }

    @Override
    public T setBool(String name, boolean v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.booleanCodec.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BOOLEAN);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setInt(int i, int v) {
        checkType(i, DataType.Name.INT);
        return setValue(i, TypeCodec.intCodec.serializeNoBoxing(v));
    }

    @Override
    public T setInt(String name, int v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.intCodec.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.INT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setLong(int i, long v) {
        checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);
        return setValue(i, TypeCodec.longCodec.serializeNoBoxing(v));
    }

    @Override
    public T setLong(String name, long v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.longCodec.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BIGINT, DataType.Name.COUNTER);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setDate(int i, Date v) {
        checkType(i, DataType.Name.TIMESTAMP);
        return setValue(i, v == null ? null : TypeCodec.dateCodec.serialize(v));
    }

    @Override
    public T setDate(String name, Date v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.dateCodec.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.TIMESTAMP);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setFloat(int i, float v) {
        checkType(i, DataType.Name.FLOAT);
        return setValue(i, TypeCodec.floatCodec.serializeNoBoxing(v));
    }

    @Override
    public T setFloat(String name, float v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.floatCodec.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.FLOAT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setDouble(int i, double v) {
        checkType(i, DataType.Name.DOUBLE);
        return setValue(i, TypeCodec.doubleCodec.serializeNoBoxing(v));
    }

    @Override
    public T setDouble(String name, double v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = TypeCodec.doubleCodec.serializeNoBoxing(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.DOUBLE);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setString(int i, String v) {
        DataType.Name type = checkType(i, DataType.Name.VARCHAR, DataType.Name.TEXT, DataType.Name.ASCII);
        switch (type) {
            case ASCII:
                return setValue(i, v == null ? null : TypeCodec.asciiStringCodec.serialize(v));
            case TEXT:
            case VARCHAR:
                return setValue(i, v == null ? null : TypeCodec.utf8StringCodec.serialize(v));
            default:
                throw new AssertionError();
        }
    }

    @Override
    public T setString(String name, String v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setString(indexes[i], v);
        return wrapped;
    }

    @Override
    public T setBytes(int i, ByteBuffer v) {
        checkType(i, DataType.Name.BLOB);
        return setBytesUnsafe(i, v);
    }

    @Override
    public T setBytes(String name, ByteBuffer v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.BLOB);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    @Override
    public T setBytesUnsafe(String name, ByteBuffer v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : v.duplicate();
        for (int i = 0; i < indexes.length; i++)
            setValue(indexes[i], value);
        return wrapped;
    }

    @Override
    public T setVarint(int i, BigInteger v) {
        checkType(i, DataType.Name.VARINT);
        return setValue(i, v == null ? null : TypeCodec.bigIntegerCodec.serialize(v));
    }

    @Override
    public T setVarint(String name, BigInteger v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.bigIntegerCodec.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.VARINT);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DataType.Name.DECIMAL);
        return setValue(i, v == null ? null : TypeCodec.decimalCodec.serialize(v));
    }

    @Override
    public T setDecimal(String name, BigDecimal v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.decimalCodec.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.DECIMAL);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setUUID(int i, UUID v) {
        DataType.Name type = checkType(i, DataType.Name.UUID, DataType.Name.TIMEUUID);

        if (v == null)
            return setValue(i, null);

        if (type == DataType.Name.TIMEUUID && v.version() != 1)
            throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));

        return type == DataType.Name.UUID
             ? setValue(i, TypeCodec.uuidCodec.serialize(v))
             : setValue(i, TypeCodec.timeUuidCodec.serialize(v));
    }

    @Override
    public T setUUID(String name, UUID v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.uuidCodec.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            DataType.Name type = checkType(indexes[i], DataType.Name.UUID, DataType.Name.TIMEUUID);
            if (v != null && type == DataType.Name.TIMEUUID && v.version() != 1)
                throw new InvalidTypeException(String.format("%s is not a Type 1 (time-based) UUID", v));
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    @Override
    public T setInet(int i, InetAddress v) {
        checkType(i, DataType.Name.INET);
        return setValue(i, v == null ? null : TypeCodec.inetCodec.serialize(v));
    }

    @Override
    public T setInet(String name, InetAddress v) {
        int[] indexes = getAllIndexesOf(name);
        ByteBuffer value = v == null ? null : TypeCodec.inetCodec.serialize(v);
        for (int i = 0; i < indexes.length; i++) {
            checkType(indexes[i], DataType.Name.INET);
            setValue(indexes[i], value);
        }
        return wrapped;
    }

    // setToken is package-private because we only want to expose it in BoundStatement
    T setToken(int i, Token v) {
        if (v == null)
            throw new NullPointerException(String.format("Cannot set a null token for column %s", getName(i)));
        checkType(i, v.getType().getName());
        return setValue(i, v.getType().codec(protocolVersion).serialize(v.getValue()));
    }

    T setToken(String name, Token v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setToken(indexes[i], v);
        return wrapped;
    }

    @Override
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

    @Override
    public <E> T setList(String name, List<E> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setList(indexes[i], v);
        return wrapped;
    }

    @Override
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

    @Override
    public <K, V> T setMap(String name, Map<K, V> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setMap(indexes[i], v);
        return wrapped;
    }

    @Override
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

    @Override
    public <E> T setSet(String name, Set<E> v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setSet(indexes[i], v);
        return wrapped;
    }

    @Override
    public T setUDTValue(int i, UDTValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.UDT)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a UDT", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // UDT always use the V3 protocol version to encode values
        setValue(i, type.codec(ProtocolVersion.V3).serialize(v));
        return wrapped;
    }

    @Override
    public T setUDTValue(String name, UDTValue v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setUDTValue(indexes[i], v);
        return wrapped;
    }

    @Override
    public T setTupleValue(int i, TupleValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.TUPLE)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a tuple", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // Tuples always user the V3 protocol version to encode values
        setValue(i, type.codec(ProtocolVersion.V3).serialize(v));
        return wrapped;
    }

    @Override
    public T setTupleValue(String name, TupleValue v) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setTupleValue(indexes[i], v);
        return wrapped;
    }

    @Override
    public T setToNull(int i) {
        return setValue(i, null);
    }

    @Override
    public T setToNull(String name) {
        int[] indexes = getAllIndexesOf(name);
        for (int i = 0; i < indexes.length; i++)
            setToNull(indexes[i]);
        return wrapped;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractData))
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
