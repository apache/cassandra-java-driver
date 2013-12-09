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

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.utils.Pair;

import java.util.Map;

/**
 * Static method to code/decode serialized data given their types.
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
            .put(DateType.instance,          DataType.timestamp())
            .put(UUIDType.instance,          DataType.uuid())
            .put(IntegerType.instance,       DataType.varint())
            .put(TimeUUIDType.instance,      DataType.timeuuid())
            .build();

    private static Map<DataType, SetType<?>> SETS;
    private static Map<DataType, ListType<?>> LISTS;
    private static ImmutableMap<Pair<DataType, DataType>, MapType<?, ?>> MAPS;


    static {
        SETS = buildSets();
        LISTS = buildLists();
        MAPS = buildMaps();
    }

    protected static ImmutableMap<Pair<DataType, DataType>, MapType<?, ?>> buildMaps() {
        ImmutableMap.Builder<Pair<DataType,DataType>, MapType<?,?>> mapsBuilder = ImmutableMap.builder();
        for (DataType typeArg1 : DataType.allPrimitiveTypes()) {
            for (DataType typeArg2 : DataType.allPrimitiveTypes()) {
                mapsBuilder.put(Pair.create(typeArg1,typeArg2), MapType.getInstance(
                        getCodec(typeArg1), getCodec(typeArg2)
                ));
            }
        }
        return mapsBuilder.build();
    }

    protected static ImmutableMap<DataType, ListType<?>> buildLists() {
        ImmutableMap.Builder<DataType, ListType<?>> listsBuilder = ImmutableMap.builder();
        for (DataType typeArg : DataType.allPrimitiveTypes()) {
            listsBuilder.put(typeArg, ListType.getInstance(getCodec(typeArg)));
        }
        return listsBuilder.build();
    }

    protected static ImmutableMap<DataType, SetType<?>> buildSets() {
        ImmutableMap.Builder<DataType, SetType<?>> setsBuilder = ImmutableMap.builder();
        for (DataType typeArg : DataType.allPrimitiveTypes()) {
            final AbstractType<Object> codec = getCodec(typeArg);
            setsBuilder.put(typeArg, SetType.getInstance(codec));
        }
        return setsBuilder.build();
    }

    
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
            case TIMESTAMP: return DateType.instance;
            case UUID:      return UUIDType.instance;
            case VARCHAR:   return UTF8Type.instance;
            case VARINT:    return IntegerType.instance;
            case TIMEUUID:  return TimeUUIDType.instance;
            case LIST:      return LISTS.get(type.getTypeArguments().get(0));
            case SET:       return SETS.get(type.getTypeArguments().get(0));
            case MAP:       return MAPS.get(Pair.create(type.getTypeArguments().get(0),
                                                        type.getTypeArguments().get(1)));
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
}
