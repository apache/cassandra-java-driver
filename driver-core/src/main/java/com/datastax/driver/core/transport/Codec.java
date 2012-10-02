package com.datastax.driver.core.transport;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.DataType;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.*;

/**
 * Static method to code/decode serialized data given their types.
 */
public class Codec {

    private static Map<AbstractType<?>, DataType.Native> rawNativeMap = new HashMap<AbstractType<?>, DataType.Native>();
    static {
        rawNativeMap.put(AsciiType.instance,         DataType.Native.ASCII);
        rawNativeMap.put(LongType.instance,          DataType.Native.BIGINT);
        rawNativeMap.put(BytesType.instance,         DataType.Native.BLOB);
        rawNativeMap.put(BooleanType.instance,       DataType.Native.BOOLEAN);
        rawNativeMap.put(CounterColumnType.instance, DataType.Native.COUNTER);
        rawNativeMap.put(DecimalType.instance,       DataType.Native.DECIMAL);
        rawNativeMap.put(DoubleType.instance,        DataType.Native.DOUBLE);
        rawNativeMap.put(FloatType.instance,         DataType.Native.FLOAT);
        rawNativeMap.put(InetAddressType.instance,   DataType.Native.INET);
        rawNativeMap.put(Int32Type.instance,         DataType.Native.INT);
        rawNativeMap.put(UTF8Type.instance,          DataType.Native.TEXT);
        rawNativeMap.put(DateType.instance,          DataType.Native.TIMESTAMP);
        rawNativeMap.put(UUIDType.instance,          DataType.Native.UUID);
        rawNativeMap.put(UTF8Type.instance,          DataType.Native.VARCHAR);
        rawNativeMap.put(IntegerType.instance,       DataType.Native.VARINT);
        rawNativeMap.put(TimeUUIDType.instance,      DataType.Native.TIMEUUID);
    }

    private Codec() {}

    public static <T> AbstractType<T> getCodec(DataType type) {
        switch (type.kind()) {
            case NATIVE:     return (AbstractType<T>)nativeCodec(type.asNative());
            case COLLECTION: return (AbstractType<T>)collectionCodec(type.asCollection());
            case CUSTOM:     return (AbstractType<T>)customCodec(type.asCustom());
            default:         throw new RuntimeException("Unknow data type kind");
        }
    }

    private static AbstractType<?> nativeCodec(DataType.Native type) {

        switch (type) {
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
            default:        throw new RuntimeException("Unknown native type");
        }
    }

    private static AbstractType<?> collectionCodec(DataType.Collection type) {

        switch (type.collectionType()) {
            case LIST:
                AbstractType<?> listElts = getCodec(((DataType.Collection.List)type).getElementsType());
                return ListType.getInstance(listElts);
            case SET:
                AbstractType<?> setElts = getCodec(((DataType.Collection.Set)type).getElementsType());
                return SetType.getInstance(setElts);
            case MAP:
                DataType.Collection.Map mt = (DataType.Collection.Map)type;
                AbstractType<?> mapKeys = getCodec(mt.getKeysType());
                AbstractType<?> mapValues = getCodec(mt.getKeysType());
                return MapType.getInstance(mapKeys, mapValues);
            default:
                throw new RuntimeException("Unknown collection type");
        }
    }

    private static AbstractType<?> customCodec(DataType.Custom type) {
        return null;
    }

    public static DataType rawTypeToDataType(AbstractType<?> rawType) {
        DataType type = rawNativeMap.get(rawType);
        if (type != null)
            return type;

        if (rawType instanceof CollectionType) {
            switch (((CollectionType)rawType).kind) {
                case LIST:
                    DataType listElts = rawTypeToDataType(((ListType)rawType).elements);
                    return new DataType.Collection.List(listElts);
                case SET:
                    DataType setElts = rawTypeToDataType(((SetType)rawType).elements);
                    return new DataType.Collection.Set(setElts);
                case MAP:
                    MapType mt = (MapType)rawType;
                    DataType mapKeys = rawTypeToDataType(mt.keys);
                    DataType mapValues = rawTypeToDataType(mt.values);
                    return new DataType.Collection.Map(mapKeys, mapValues);
                default:
                    throw new RuntimeException("Unknown collection type");
            }
        }

        // TODO: handle custom
        return null;
    }
}
