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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.cassandra.db.marshal.*;

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

    private static Map<DataType.Name, SetType<?>> SETS = buildSets();
    private static Map<DataType.Name, ListType<?>> LISTS = buildLists();
    private static Map<DataType.Name, Map<DataType.Name, MapType<?, ?>>> MAPS = buildMaps();

    protected static Map<DataType.Name, Map<DataType.Name, MapType<?, ?>>> buildMaps() {

        ImmutableMap.Builder<DataType.Name, Map<DataType.Name, MapType<?, ?>>> mapsBuilder = ImmutableMap.builder();

        for (DataType typeArg1 : DataType.allPrimitiveTypes()) {
            ImmutableMap.Builder<DataType.Name, MapType<?, ?>> innerMapsBuilder = ImmutableMap.builder();
            for (DataType typeArg2 : DataType.allPrimitiveTypes()) {
                innerMapsBuilder.put(
                        typeArg2.getName(),
                        MapType.getInstance(getCodec(typeArg1), getCodec(typeArg2))
                );
            }
            mapsBuilder.put(typeArg1.getName(), Maps.newEnumMap(innerMapsBuilder.build()));
        }
        return Maps.newEnumMap(mapsBuilder.build());
    }

    protected static ImmutableMap<DataType.Name, ListType<?>> buildLists() {
        ImmutableMap.Builder<DataType.Name, ListType<?>> listsBuilder = ImmutableMap.builder();
        for (DataType typeArg : DataType.allPrimitiveTypes()) {
            listsBuilder.put(typeArg.getName(), ListType.getInstance(getCodec(typeArg)));
        }
        return Maps.immutableEnumMap(listsBuilder.build());
    }

    protected static ImmutableMap<DataType.Name, SetType<?>> buildSets() {
        ImmutableMap.Builder<DataType.Name, SetType<?>> setsBuilder = ImmutableMap.builder();
        for (DataType typeArg : DataType.allPrimitiveTypes()) {
            final AbstractType<Object> codec = getCodec(typeArg);
            setsBuilder.put(typeArg.getName(), SetType.getInstance(codec));
        }
        return Maps.immutableEnumMap(setsBuilder.build());
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
            case LIST:      return getListType(type);
            case SET:       return getSetType(type);
            case MAP:       return getMapType(type);
            // We don't interpret custom values in any way
            case CUSTOM:    return BytesType.instance;
            default:        throw new RuntimeException("Unknown type");
        }
    }

    private static MapType<?, ?> getMapType(DataType type) {
        final DataType dataTypeArg0 = type.getTypeArguments().get(0);
        final DataType dateTypeArg1 = type.getTypeArguments().get(1);

        Map<DataType.Name, MapType<?, ?>> tmp = MAPS.get(dataTypeArg0.getName());
        MapType<?, ?> map = tmp == null ? null : tmp.get(dateTypeArg1.getName());

        return map == null ? MapType.getInstance(getCodecInternal(dataTypeArg0), getCodecInternal(dateTypeArg1)) : map;
    }

    private static SetType<?> getSetType(DataType type) {
        final DataType dataTypeArg0 = type.getTypeArguments().get(0);

        SetType<?> set = SETS.get(dataTypeArg0.getName());
        return set == null ? SetType.getInstance(getCodecInternal(dataTypeArg0)) : set;
    }

    private static ListType<?> getListType(final DataType type) {
        final DataType dataTypeArg0 = type.getTypeArguments().get(0);

        ListType<?> list = LISTS.get(dataTypeArg0.getName());
        return list == null ? ListType.getInstance(getCodecInternal(dataTypeArg0)) : list;
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
