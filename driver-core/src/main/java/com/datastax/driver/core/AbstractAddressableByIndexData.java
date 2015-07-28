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

import com.google.common.base.Objects;
import com.google.common.reflect.TypeToken;

import static com.datastax.driver.core.CodecUtils.listOf;
import static com.datastax.driver.core.CodecUtils.mapOf;
import static com.datastax.driver.core.CodecUtils.setOf;
import static com.datastax.driver.core.DataType.Name.*;

abstract class AbstractAddressableByIndexData<T extends SettableByIndexData<T>> extends AbstractGettableByIndexData implements SettableByIndexData<T> {

    final ByteBuffer[] values;

    protected AbstractAddressableByIndexData(ProtocolVersion protocolVersion, int size) {
        super(protocolVersion);
        this.values = new ByteBuffer[size];
    }

    @SuppressWarnings("unchecked")
    protected T setValue(int i, ByteBuffer value) {
        values[i] = value;
        return (T)this;
    }

    protected ByteBuffer getValue(int i) {
        return values[i];
    }

    public T setBool(int i, boolean v) {
        checkType(i, BOOLEAN);
        TypeCodec<Boolean> codec = codecFor(i, Boolean.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveBooleanCodec)
            bb = ((TypeCodec.PrimitiveBooleanCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setByte(int i, byte v) {
        checkType(i, TINYINT);
        TypeCodec<Byte> codec = codecFor(i, Byte.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveByteCodec)
            bb = ((TypeCodec.PrimitiveByteCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setShort(int i, short v) {
        checkType(i, SMALLINT);
        TypeCodec<Short> codec = codecFor(i, Short.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveShortCodec)
            bb = ((TypeCodec.PrimitiveShortCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setInt(int i, int v) {
        checkType(i, INT);
        TypeCodec<Integer> codec = codecFor(i, Integer.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveIntCodec)
            bb = ((TypeCodec.PrimitiveIntCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setLong(int i, long v) {
        checkType(i, BIGINT, COUNTER);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setTimestamp(int i, Date v) {
        checkType(i, TIMESTAMP);
        return setValue(i, codecFor(i, Date.class).serialize(v, protocolVersion));
    }

    public T setDate(int i, LocalDate v) {
        checkType(i, DATE);
        return setValue(i, codecFor(i, LocalDate.class).serialize(v, protocolVersion));
    }

    public T setTime(int i, long v) {
        checkType(i, TIME);
        TypeCodec<Long> codec = codecFor(i, Long.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveLongCodec)
            bb = ((TypeCodec.PrimitiveLongCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setFloat(int i, float v) {
        checkType(i, FLOAT);
        TypeCodec<Float> codec = codecFor(i, Float.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveFloatCodec)
            bb = ((TypeCodec.PrimitiveFloatCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setDouble(int i, double v) {
        checkType(i, DOUBLE);
        TypeCodec<Double> codec = codecFor(i, Double.class);
        ByteBuffer bb;
        if(codec instanceof TypeCodec.PrimitiveDoubleCodec)
            bb = ((TypeCodec.PrimitiveDoubleCodec)codec).serializeNoBoxing(v, protocolVersion);
        else
            bb = codec.serialize(v, protocolVersion);
        return setValue(i, bb);
    }

    public T setString(int i, String v) {
        checkType(i, VARCHAR, TEXT, ASCII);
        return setValue(i, codecFor(i, String.class).serialize(v, protocolVersion));
    }

    public T setBytes(int i, ByteBuffer v) {
        checkType(i, BLOB);
        return setValue(i, codecFor(i, ByteBuffer.class).serialize(v, protocolVersion));
    }

    public T setBytesUnsafe(int i, ByteBuffer v) {
        return setValue(i, v == null ? null : v.duplicate());
    }

    public T setVarint(int i, BigInteger v) {
        checkType(i, VARINT);
        return setValue(i, codecFor(i, BigInteger.class).serialize(v, protocolVersion));
    }

    public T setDecimal(int i, BigDecimal v) {
        checkType(i, DECIMAL);
        return setValue(i, codecFor(i, BigDecimal.class).serialize(v, protocolVersion));
    }

    public T setUUID(int i, UUID v) {
        checkType(i, UUID, TIMEUUID);
        return setValue(i, codecFor(i, UUID.class).serialize(v, protocolVersion));
    }

    public T setInet(int i, InetAddress v) {
        checkType(i, INET);
        return setValue(i, codecFor(i, InetAddress.class).serialize(v, protocolVersion));
    }

    @SuppressWarnings("unchecked")
    public <E> T setList(int i, List<E> v) {
        checkType(i, LIST);
        if(v == null || v.isEmpty()) {
            // no runtime inspection possible, rely on the underlying metadata
            return setValue(i, codecFor(i).serialize(v, protocolVersion));
        } else {
            // inspect the first element and locate a codec that accepts both the underlying CQL type and the actual Java type
            DataType eltCqlType = getType(i).getTypeArguments().get(0);
            TypeToken<E> eltJavaType = getCodecRegistry().codecFor(eltCqlType, v.iterator().next()).getJavaType();
            return setValue(i, codecFor(i, listOf(eltJavaType)).serialize(v, protocolVersion));
        }
    }

    @Override
    public <E> T setList(int i, List<E> v, Class<E> elementsClass) {
        return setValue(i, codecFor(i, listOf(elementsClass)).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setList(int i, List<E> v, TypeToken<E> elementsType) {
        return setValue(i, codecFor(i, listOf(elementsType)).serialize(v, protocolVersion));
    }

    @SuppressWarnings("unchecked")
    public <K, V> T setMap(int i, Map<K, V> v) {
        checkType(i, MAP);
        if(v == null || v.isEmpty()) {
            // no runtime inspection possible, rely on the underlying metadata
            return setValue(i, codecFor(i).serialize(v, protocolVersion));
        } else {
            // inspect the first element and locate a codec that accepts both the underlying CQL type and the actual Java types for keys and values
            DataType keysCqlType = getType(i).getTypeArguments().get(0);
            DataType valuesCqlType = getType(i).getTypeArguments().get(1);
            Map.Entry<K, V> entry = v.entrySet().iterator().next();
            TypeToken<K> keysType = getCodecRegistry().codecFor(keysCqlType, entry.getKey()).getJavaType();
            TypeToken<V> valuesType = getCodecRegistry().codecFor(valuesCqlType, entry.getValue()).getJavaType();
            return setValue(i, codecFor(i, mapOf(keysType, valuesType)).serialize(v, protocolVersion));
        }
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, Class<K> keysClass, Class<V> valuesClass) {
        return setValue(i, codecFor(i, mapOf(keysClass, valuesClass)).serialize(v, protocolVersion));
    }

    @Override
    public <K, V> T setMap(int i, Map<K, V> v, TypeToken<K> keysType, TypeToken<V> valuesType) {
        return setValue(i, codecFor(i, mapOf(keysType, valuesType)).serialize(v, protocolVersion));
    }

    @SuppressWarnings("unchecked")
    public <E> T setSet(int i, Set<E> v) {
        checkType(i, SET);
        if(v == null || v.isEmpty()) {
            // no runtime inspection possible, rely on the underlying metadata
            return setValue(i, codecFor(i).serialize(v, protocolVersion));
        } else {
            // inspect the first element and locate a codec that accepts both the underlying CQL type and the actual Java type
            DataType eltCqlType = getType(i).getTypeArguments().get(0);
            TypeToken<E> eltJavaType = getCodecRegistry().codecFor(eltCqlType, v.iterator().next()).getJavaType();
            return setValue(i, codecFor(i, setOf(eltJavaType)).serialize(v, protocolVersion));
        }
    }

    @Override
    public <E> T setSet(int i, Set<E> v, Class<E> elementsClass) {
        return setValue(i, codecFor(i, setOf(elementsClass)).serialize(v, protocolVersion));
    }

    @Override
    public <E> T setSet(int i, Set<E> v, TypeToken<E> elementsType) {
        return setValue(i, codecFor(i, setOf(elementsType)).serialize(v, protocolVersion));
    }

    public T setUDTValue(int i, UDTValue v) {
        checkType(i, UDT);
        return setValue(i, codecFor(i, UDTValue.class).serialize(v, protocolVersion));
    }

    public T setTupleValue(int i, TupleValue v) {
        checkType(i, TUPLE);
        return setValue(i, codecFor(i, TupleValue.class).serialize(v, protocolVersion));
    }

    @Override
    public <V> T setObject(int i, V v) {
        TypeCodec<V> codec = v == null ? this.<V>codecFor(i) : codecFor(i, v);
        return set(i, v, codec);
    }

    @Override
    public <V> T set(int i, V v, Class<V> targetClass) {
        return set(i, v, codecFor(i, targetClass));
    }

    @Override
    public <V> T set(int i, V v, TypeToken<V> targetType) {
        return set(i, v, codecFor(i, targetType));
    }

    @Override
    public <V> T set(int i, V v, TypeCodec<V> codec) {
        checkType(i, codec.getCqlType().getName());
        return setValue(i, codec.serialize(v, protocolVersion));
    }

    public T setToNull(int i) {
        return setValue(i, null);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof AbstractAddressableByIndexData))
            return false;

        AbstractAddressableByIndexData<?> that = (AbstractAddressableByIndexData<?>)o;
        if (values.length != that.values.length)
            return false;

        if(this.protocolVersion != that.protocolVersion)
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

            Object thisValue = this.codecFor(i).deserialize(this.values[i], this.protocolVersion);
            Object thatValue = that.codecFor(i).deserialize(that.values[i], that.protocolVersion);
            if (!Objects.equal(thisValue, thatValue))
                return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        // Same as equals
        int hash = 31;
        for (int i = 0; i < values.length; i++)
            hash += values[i] == null ? 1 : codecFor(i).deserialize(values[i], protocolVersion).hashCode();
        return hash;
    }
}
