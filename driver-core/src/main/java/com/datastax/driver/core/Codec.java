package com.datastax.driver.core;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.exceptions.DriverInternalError;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.db.marshal.*;

/**
 * Static method to code/decode serialized data given their types.
 */
class Codec {

    private static Map<AbstractType<?>, DataType> rawNativeMap = new HashMap<AbstractType<?>, DataType>() {{
        put(AsciiType.instance,         DataType.ascii());
        put(LongType.instance,          DataType.bigint());
        put(BytesType.instance,         DataType.blob());
        put(BooleanType.instance,       DataType.cboolean());
        put(CounterColumnType.instance, DataType.counter());
        put(DecimalType.instance,       DataType.decimal());
        put(DoubleType.instance,        DataType.cdouble());
        put(FloatType.instance,         DataType.cfloat());
        put(InetAddressType.instance,   DataType.inet());
        put(Int32Type.instance,         DataType.cint());
        put(UTF8Type.instance,          DataType.text());
        put(DateType.instance,          DataType.timestamp());
        put(UUIDType.instance,          DataType.uuid());
        put(IntegerType.instance,       DataType.varint());
        put(TimeUUIDType.instance,      DataType.timeuuid());
    }};

    private Codec() {}

    public static <T> AbstractType<T> getCodec(DataType type) {
        return (AbstractType<T>)getCodecInternal(type);
    }

    private static AbstractType getCodecInternal(DataType type) {
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
            case LIST:      return ListType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case SET:       return SetType.getInstance(getCodec(type.getTypeArguments().get(0)));
            case MAP:       return MapType.getInstance(getCodec(type.getTypeArguments().get(0)), getCodec(type.getTypeArguments().get(1)));
            default:        throw new RuntimeException("Unknown type");
        }
    }

    public static DataType rawTypeToDataType(AbstractType<?> rawType) {
        DataType type = rawNativeMap.get(rawType);
        if (type != null)
            return type;

        if (rawType instanceof CollectionType) {
            switch (((CollectionType)rawType).kind) {
                case LIST:
                    return DataType.list(rawTypeToDataType(((ListType)rawType).elements));
                case SET:
                    return DataType.set(rawTypeToDataType(((SetType)rawType).elements));
                case MAP:
                    MapType mt = (MapType)rawType;
                    return DataType.map(rawTypeToDataType(mt.keys), rawTypeToDataType(mt.values));
            }
        }
        throw new DriverInternalError("Unsupported type: " + rawType);
    }

    // Returns whether type can be safely subtyped to klass
    public static boolean isCompatibleSubtype(DataType type, Class klass) {
        switch (type.getName()) {
            case ASCII:     return klass.isAssignableFrom(String.class);
            case BIGINT:    return klass.isAssignableFrom(Long.class);
            case BLOB:      return klass.isAssignableFrom(ByteBuffer.class);
            case BOOLEAN:   return klass.isAssignableFrom(Boolean.class);
            case COUNTER:   return klass.isAssignableFrom(Long.class);
            case DECIMAL:   return klass.isAssignableFrom(BigDecimal.class);
            case DOUBLE:    return klass.isAssignableFrom(Double.class);
            case FLOAT:     return klass.isAssignableFrom(Float.class);
            case INET:      return klass.isAssignableFrom(InetAddress.class);
            case INT:       return klass.isAssignableFrom(Integer.class);
            case TEXT:      return klass.isAssignableFrom(String.class);
            case TIMESTAMP: return klass.isAssignableFrom(Date.class);
            case UUID:      return klass.isAssignableFrom(UUID.class);
            case VARCHAR:   return klass.isAssignableFrom(String.class);
            case VARINT:    return klass.isAssignableFrom(BigInteger.class);
            case TIMEUUID:  return klass.isAssignableFrom(UUID.class);
            default:        throw new RuntimeException("Unknown non-collection type " + type);
        }
    }

    // Returns whether klass can be safely subtyped to klass, i.e. if type is a supertype of klass
    public static boolean isCompatibleSupertype(DataType type, Class klass) {
        switch (type.getName()) {
            case ASCII:     return String.class.isAssignableFrom(klass);
            case BIGINT:    return Long.class.isAssignableFrom(klass);
            case BLOB:      return ByteBuffer.class.isAssignableFrom(klass);
            case BOOLEAN:   return Boolean.class.isAssignableFrom(klass);
            case COUNTER:   return Long.class.isAssignableFrom(klass);
            case DECIMAL:   return BigDecimal.class.isAssignableFrom(klass);
            case DOUBLE:    return Double.class.isAssignableFrom(klass);
            case FLOAT:     return Float.class.isAssignableFrom(klass);
            case INET:      return InetAddress.class.isAssignableFrom(klass);
            case INT:       return Integer.class.isAssignableFrom(klass);
            case TEXT:      return String.class.isAssignableFrom(klass);
            case TIMESTAMP: return Date.class.isAssignableFrom(klass);
            case UUID:      return UUID.class.isAssignableFrom(klass);
            case VARCHAR:   return String.class.isAssignableFrom(klass);
            case VARINT:    return BigInteger.class.isAssignableFrom(klass);
            case TIMEUUID:  return UUID.class.isAssignableFrom(klass);
            default:        throw new RuntimeException("Unknown non-collection type " + type);
        }
    }
}
