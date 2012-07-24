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

    public static AbstractType<?> getCodec(DataType type) {
        switch (type.kind()) {
            case NATIVE:     return nativeCodec(type.asNative());
            case COLLECTION: return collectionCodec(type.asCollection());
            case CUSTOM:     return customCodec(type.asCustom());
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
        return null;
    }

    private static AbstractType<?> customCodec(DataType.Custom type) {
        return null;
    }

    public static DataType rawTypeToDataType(AbstractType<?> rawType) {
        DataType type = rawNativeMap.get(rawType);
        if (type != null)
            return type;

        // TODO: handle collections and custom
        return null;
    }
}
