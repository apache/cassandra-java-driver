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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

abstract class AbstractAddressableByIndexData<T extends SettableByIndexData<T>> extends AbstractGettableByIndexData implements SettableByIndexData<T> {

    final ByteBuffer[] values;

    protected AbstractAddressableByIndexData(ProtocolVersion protocolVersion, int size) {
        super(protocolVersion);
        this.values = new ByteBuffer[size];
    }

    @SuppressWarnings("unchecked")
    protected T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return (T) this;
    }

    @Override
    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    @Override
    public T setBool(int i, boolean v) {
        checkType(i, DataType.Name.BOOLEAN);
        return setValue(i, TypeCodec.booleanCodec.serializeNoBoxing(v));
    }

    @Override
    public T setInt(int i, int v) {
        checkType(i, DataType.Name.INT);
        return setValue(i, TypeCodec.intCodec.serializeNoBoxing(v));
    }

    @Override
    public T setLong(int i, long v) {
        checkType(i, DataType.Name.BIGINT, DataType.Name.COUNTER);
        return setValue(i, TypeCodec.longCodec.serializeNoBoxing(v));
    }

    @Override
    public T setDate(int i, Date v) {
        checkType(i, DataType.Name.TIMESTAMP);
        return setValue(i, v == null ? null : TypeCodec.dateCodec.serialize(v));
    }

    @Override
    public T setFloat(int i, float v) {
        checkType(i, DataType.Name.FLOAT);
        return setValue(i, TypeCodec.floatCodec.serializeNoBoxing(v));
    }

    @Override
    public T setDouble(int i, double v) {
        checkType(i, DataType.Name.DOUBLE);
        return setValue(i, TypeCodec.doubleCodec.serializeNoBoxing(v));
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
    public T setBytes(int i, ByteBuffer v) {
        checkType(i, DataType.Name.BLOB);
        return setBytesUnsafe(i, v);
    }

    @Override
    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    @Override
    public T setVarint(int i, BigInteger v) {
        checkType(i, DataType.Name.VARINT);
        return setValue(i, v == null ? null : TypeCodec.bigIntegerCodec.serialize(v));
    }

    @Override
    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DataType.Name.DECIMAL);
        return setValue(i, v == null ? null : TypeCodec.decimalCodec.serialize(v));
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
    public T setInet(int i, InetAddress v) {
        checkType(i, DataType.Name.INET);
        return setValue(i, v == null ? null : TypeCodec.inetCodec.serialize(v));
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
    public T setUDTValue(int i, UDTValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.UDT)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a UDT", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // UDT always use the V3 protocol version to encode values
        return setValue(i, type.codec(ProtocolVersion.V3).serialize(v));
    }

    @Override
    public T setTupleValue(int i, TupleValue v) {
        DataType type = getType(i);
        if (type.getName() != DataType.Name.TUPLE)
            throw new InvalidTypeException(String.format("Column %s is of type %s, cannot set to a tuple", getName(i), type));

        if (v == null)
            return setValue(i, null);

        // Tuples always user the V3 protocol version to encode values
        return setValue(i, type.codec(ProtocolVersion.V3).serialize(v));
    }

    @Override
    public T setToNull(int i) {
        return setValue(i, null);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractAddressableByIndexData))
            return false;

        AbstractAddressableByIndexData<?> that = (AbstractAddressableByIndexData<?>) o;
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
