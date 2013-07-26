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

import com.datastax.driver.core.DataType;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.marshal.*;

/**
 * Static method to code/decode serialized data given their types.
 *
 * This is *not* meant to be exposed publicly.
 */
class Codec {

    private static ImmutableMap<AbstractType<?>, DataType> rawNativeMap =
        new ImmutableMap.Builder<AbstractType<?>, DataType>()
            .put(AsciiType.instance,         DataType.ascii())
            .put(LongType.instance,          DataType.bigint())
            .put(BytesType.instance,         DataType.blob())
            .put(BooleanType.instance,       DataType.cboolean())
            .put(CounterColumnType.instance, DataType.counter())
            .put(DecimalType.instance,       DataType.decimal())
            .put(DoubleType.instance,        DataType.cdouble())
            .put(FloatType.instance,         DataType.cfloat())
            .put(InetAddressType.instance,   DataType.inet())
            .put(Int32Type.instance,         DataType.cint())
            .put(UTF8Type.instance,          DataType.text())
            .put(TimestampType.instance,     DataType.timestamp())
            .put(UUIDType.instance,          DataType.uuid())
            .put(IntegerType.instance,       DataType.varint())
            .put(TimeUUIDType.instance,      DataType.timeuuid())
            .build();

    private Codec() {}

    @SuppressWarnings("unchecked")
    public static <T> AbstractType<T> getCodec(DataType type) {
        return (AbstractType<T>)getCodecInternal(type);
    }

    private static AbstractType<?> getCodecInternal(DataType type) {
        switch (type.getName()) {
            case ASCII:     return AsciiType.instance;
            case BIGINT:    return LongType.instance;
            case BLOB:      return BytesType.instance;
            case BOOLEAN:   return BooleanType.instance;
            case COUNTER:   return CounterColumnType.instance;
            case DECIMAL:   return DecimalType.instance;
            case DOUBLE:    return DoubleType.instance;
            case FLOAT:     return FloatType.instance;
            case INET:      return InetAddressType.instance;
            case INT:       return Int32Type.instance;
            case TEXT:      return UTF8Type.instance;
            case TIMESTAMP: return TimestampType.instance;
            case UUID:      return UUIDType.instance;
            case VARCHAR:   return UTF8Type.instance;
            case VARINT:    return IntegerType.instance;
            case TIMEUUID:  return TimeUUIDType.instance;
            case LIST:      return ListType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case SET:       return SetType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case MAP:       return MapType.getInstance(getCodec(type.getTypeArguments().get(0)), getCodec(type.getTypeArguments().get(1)));
            // We don't interpret custom values in any way
            case CUSTOM:    return BytesType.instance;
            default:        throw new RuntimeException("Unknown type");
        }
    }

    public static DataType rawTypeToDataType(AbstractType<?> rawType) {
        if (rawType instanceof ReversedType<?>)
            rawType = ((ReversedType<?>) rawType).baseType;

        DataType type = rawNativeMap.get(rawType);
        if (type != null)
            return type;

        if (rawType instanceof CollectionType<?>) {
            switch (((CollectionType<?>)rawType).kind) {
                case LIST:
                    return DataType.list(rawTypeToDataType(((ListType<?>)rawType).elements));
                case SET:
                    return DataType.set(rawTypeToDataType(((SetType<?>)rawType).elements));
                case MAP:
                    MapType<?, ?> mt = (MapType<?, ?>)rawType;
                    return DataType.map(rawTypeToDataType(mt.keys), rawTypeToDataType(mt.values));
            }
        }
        return DataType.custom(rawType.getClass().toString());
    }

    /* This is ugly, but not sure how we can do much better/faster
     * Returns if it's doesn't correspond to a known type.
     *
     * Also, note that this only a dataType that is fit for the value,
     * but for instance, for a UUID, this will return never DataType.uuid() but
     * never DataType.timeuuid(). Also, provided an empty list, this will return
     * DataType.list(DataType.blob()), which is semi-random. This is ok if all
     * we want is serialize the value, but that's probably all we should do with
     * the return of this method.
     */
    public static DataType getDataTypeFor(Object value) {
        // Starts with ByteBuffer, so that if already serialized value are provided, we don't have the
        // cost of tested a bunch of other types first
        if (value instanceof ByteBuffer)
            return DataType.blob();

        if (value instanceof Number) {
            if (value instanceof Integer)
                return DataType.cint();
            if (value instanceof Long)
                return DataType.bigint();
            if (value instanceof Float)
                return DataType.cfloat();
            if (value instanceof Double)
                return DataType.cdouble();
            if (value instanceof BigDecimal)
                return DataType.decimal();
            if (value instanceof BigInteger)
                return DataType.decimal();
            return null;
        }

        if (value instanceof String)
            return DataType.text();

        if (value instanceof Boolean)
            return DataType.cboolean();

        if (value instanceof InetAddress)
            return DataType.inet();

        if (value instanceof Date)
            return DataType.timestamp();

        if (value instanceof UUID)
            return DataType.uuid();

        if (value instanceof List) {
            List<?> l = (List<?>)value;
            if (l.isEmpty())
                return DataType.list(DataType.blob());
            DataType eltType = getDataTypeFor(l.get(0));
            return eltType == null ? null : DataType.list(eltType);
        }

        if (value instanceof Set) {
            Set<?> s = (Set<?>)value;
            if (s.isEmpty())
                return DataType.set(DataType.blob());
            DataType eltType = getDataTypeFor(s.iterator().next());
            return eltType == null ? null : DataType.set(eltType);
        }

        if (value instanceof Map) {
            Map<?, ?> m = (Map<?, ?>)value;
            if (m.isEmpty())
                return DataType.map(DataType.blob(), DataType.blob());
            Map.Entry<?, ?> e = m.entrySet().iterator().next();
            DataType keyType = getDataTypeFor(e.getKey());
            DataType valueType = getDataTypeFor(e.getValue());
            return keyType == null || valueType == null
                 ? null
                 : DataType.map(keyType, valueType);
        }

        return null;
    }
}
