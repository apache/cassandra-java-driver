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
import java.net.UnknownHostException;
import java.nio.*;
import java.nio.charset.*;
import java.text.*;
import java.util.*;
import java.util.regex.Pattern;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

abstract class TypeCodec<T> {

    // Somehow those don't seem to get properly initialized if they're not here. The reason
    // escape me right now so let's just leave it here for now
    public static final StringCodec utf8Instance = new StringCodec(Charset.forName("UTF-8"));
    public static final StringCodec asciiInstance = new StringCodec(Charset.forName("US-ASCII"));

    private static final Map<DataType.Name, TypeCodec<?>> primitiveCodecs = new EnumMap<DataType.Name, TypeCodec<?>>(DataType.Name.class);
    static {
        primitiveCodecs.put(DataType.Name.ASCII,     StringCodec.asciiInstance);
        primitiveCodecs.put(DataType.Name.BIGINT,    LongCodec.instance);
        primitiveCodecs.put(DataType.Name.BLOB,      BytesCodec.instance);
        primitiveCodecs.put(DataType.Name.BOOLEAN,   BooleanCodec.instance);
        primitiveCodecs.put(DataType.Name.COUNTER,   LongCodec.instance);
        primitiveCodecs.put(DataType.Name.DECIMAL,   DecimalCodec.instance);
        primitiveCodecs.put(DataType.Name.DOUBLE,    DoubleCodec.instance);
        primitiveCodecs.put(DataType.Name.FLOAT,     FloatCodec.instance);
        primitiveCodecs.put(DataType.Name.INET,      InetCodec.instance);
        primitiveCodecs.put(DataType.Name.TINYINT,   TinyIntCodec.instance);
        primitiveCodecs.put(DataType.Name.SMALLINT,  SmallIntCodec.instance);
        primitiveCodecs.put(DataType.Name.INT,       IntCodec.instance);
        primitiveCodecs.put(DataType.Name.TEXT,      StringCodec.utf8Instance);
        primitiveCodecs.put(DataType.Name.TIMESTAMP, DateCodec.instance);
        primitiveCodecs.put(DataType.Name.UUID,      UUIDCodec.instance);
        primitiveCodecs.put(DataType.Name.VARCHAR,   StringCodec.utf8Instance);
        primitiveCodecs.put(DataType.Name.VARINT,    BigIntegerCodec.instance);
        primitiveCodecs.put(DataType.Name.TIMEUUID,  TimeUUIDCodec.instance);
        primitiveCodecs.put(DataType.Name.CUSTOM,    BytesCodec.instance);
    }

    private static class PrimitiveCollectionCodecs
    {
        public final Map<DataType.Name, TypeCodec<List<?>>> primitiveListsCodecs = new EnumMap<DataType.Name, TypeCodec<List<?>>>(DataType.Name.class);
        public final Map<DataType.Name, TypeCodec<Set<?>>> primitiveSetsCodecs = new EnumMap<DataType.Name, TypeCodec<Set<?>>>(DataType.Name.class);
        public final Map<DataType.Name, Map<DataType.Name, TypeCodec<Map<?, ?>>>> primitiveMapsCodecs = new EnumMap<DataType.Name, Map<DataType.Name, TypeCodec<Map<?, ?>>>>(DataType.Name.class);

        @SuppressWarnings({"unchecked", "rawtypes"})
        public PrimitiveCollectionCodecs(ProtocolVersion protocolVersion)
        {
            for (Map.Entry<DataType.Name, TypeCodec<?>> entry : primitiveCodecs.entrySet()) {
                DataType.Name type = entry.getKey();
                TypeCodec<?> codec = entry.getValue();
                primitiveListsCodecs.put(type, new ListCodec(codec, protocolVersion));
                primitiveSetsCodecs.put(type, new SetCodec(codec, protocolVersion));
                Map<DataType.Name, TypeCodec<Map<?, ?>>> valueMap = new EnumMap<DataType.Name, TypeCodec<Map<?, ?>>>(DataType.Name.class);
                for (Map.Entry<DataType.Name, TypeCodec<?>> valueEntry : primitiveCodecs.entrySet())
                    valueMap.put(valueEntry.getKey(), new MapCodec(codec, valueEntry.getValue(), protocolVersion));
                primitiveMapsCodecs.put(type, valueMap);
            }
        }

        private static final PrimitiveCollectionCodecs primitiveCollectionCodecsV2 = new PrimitiveCollectionCodecs(ProtocolVersion.V2);
        private static final PrimitiveCollectionCodecs primitiveCollectionCodecsV3 = new PrimitiveCollectionCodecs(ProtocolVersion.V3);
        private static final PrimitiveCollectionCodecs primitiveCollectionCodecsV4 = new PrimitiveCollectionCodecs(ProtocolVersion.V4);

        static PrimitiveCollectionCodecs forVersion(ProtocolVersion version) {
            // This happens during protocol negociation, when the version is not known yet.
            // Use the smallest supported version, which is enough for what we need to do at this stage.
            if (version == null)
                version = ProtocolVersion.V1;

            switch (version) {
                case V1:
                case V2:
                    return primitiveCollectionCodecsV2;
                case V3:
                    return primitiveCollectionCodecsV3;
                case V4:
                    return primitiveCollectionCodecsV4;
                default:
                    throw version.unsupported();
            }
        }
    }


    private TypeCodec() {}

    public abstract T parse(String value);
    public abstract String format(T value);

    public abstract ByteBuffer serialize(T value);
    public abstract T deserialize(ByteBuffer bytes);

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<T> createFor(DataType.Name name) {
        assert !name.isCollection();
        return (TypeCodec<T>)primitiveCodecs.get(name);
    }

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<List<T>> listOf(DataType arg, ProtocolVersion protocolVersion) {
        PrimitiveCollectionCodecs codecs = PrimitiveCollectionCodecs.forVersion(protocolVersion);
        TypeCodec<List<?>> codec = codecs.primitiveListsCodecs.get(arg.getName());
        return codec != null ? (TypeCodec)codec : new ListCodec<Object>(arg.codec(protocolVersion), protocolVersion);
    }

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<Set<T>> setOf(DataType arg, ProtocolVersion protocolVersion) {
        PrimitiveCollectionCodecs codecs = PrimitiveCollectionCodecs.forVersion(protocolVersion);
        TypeCodec<Set<?>> codec = codecs.primitiveSetsCodecs.get(arg.getName());
        return codec != null ? (TypeCodec)codec : new SetCodec<Object>(arg.codec(protocolVersion), protocolVersion);
    }

    @SuppressWarnings("unchecked")
    static <K, V> TypeCodec<Map<K, V>> mapOf(DataType keys, DataType values, ProtocolVersion protocolVersion) {
        PrimitiveCollectionCodecs codecs = PrimitiveCollectionCodecs.forVersion(protocolVersion);
        Map<DataType.Name, TypeCodec<Map<?, ?>>> valueCodecs = codecs.primitiveMapsCodecs.get(keys.getName());
        TypeCodec<Map<?, ?>> codec = valueCodecs == null ? null : valueCodecs.get(values.getName());
        return codec != null ? (TypeCodec)codec : new MapCodec<Object, Object>(keys.codec(protocolVersion), values.codec(protocolVersion), protocolVersion);
    }

    static UDTCodec udtOf(UserType definition, ProtocolVersion protocolVersion) {
        return new UDTCodec(definition, protocolVersion);
    }

    static TupleCodec tupleOf(TupleType type, ProtocolVersion protocolVersion) {
        return new TupleCodec(type, protocolVersion);
    }

    /* This is ugly, but not sure how we can do much better/faster
     * Returns null if it's doesn't correspond to a known type.
     *
     * Also, note that this only a dataType that is fit for the value,
     * but for instance, for a UUID, this will return DataType.uuid() but
     * never DataType.timeuuid(). Also, provided an empty list, this will return
     * DataType.list(DataType.blob()), which is semi-random. This is ok if all
     * we want is serialize the value, but that's probably all we should do with
     * the return of this method.
     */
    static DataType getDataTypeFor(Object value) {
        // Starts with ByteBuffer, so that if already serialized value are provided, we don't have the
        // cost of testing a bunch of other types first
        if (value instanceof ByteBuffer)
            return DataType.blob();

        if (value instanceof Number) {
            if (value instanceof Byte)
                return DataType.tinyint();
            if (value instanceof Short)
                return DataType.smallint();
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
                return DataType.varint();
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

        if (value instanceof UDTValue) {
            return ((UDTValue) value).getType();
        }

        if (value instanceof TupleValue) {
            return ((TupleValue) value).getType();
        }

        return null;
    }

    private static ByteBuffer pack(List<ByteBuffer> buffers, int elements, ProtocolVersion version) {
        int size = 0;
        for (ByteBuffer bb : buffers) {
            int elemSize = sizeOfValue(bb, version);
            size += elemSize;
        }

        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize(elements, version) + size);
        writeCollectionSize(result, elements, version);
        for (ByteBuffer bb : buffers)
            writeCollectionValue(result, bb, version);
        return (ByteBuffer)result.flip();
    }

    private static void writeCollectionSize(ByteBuffer output, int elements, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                if (elements > 65535)
                    throw new IllegalArgumentException("Native protocol version 2 supports up to 65535 elements in any collection - but collection contains " + elements + " elements");
                output.putShort((short)elements);
                break;
            case V3:
            case V4:
                output.putInt(elements);
                break;
            default:
                throw version.unsupported();
        }
    }

    private static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    private static int readCollectionSize(ByteBuffer input, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                return getUnsignedShort(input);
            case V3:
            case V4:
                return input.getInt();
            default:
                throw version.unsupported();
        }
    }

    private static int sizeOfCollectionSize(int elements, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                return 2;
            case V3:
            case V4:
                return 4;
            default:
                throw version.unsupported();
        }
    }

    private static void writeCollectionValue(ByteBuffer output, ByteBuffer value, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                assert value != null;
                output.putShort((short)value.remaining());
                output.put(value.duplicate());
                break;
            case V3:
            case V4:
                if (value == null) {
                    output.putInt(-1);
                } else {
                    output.putInt(value.remaining());
                    output.put(value.duplicate());
                }
                break;
            default:
                throw version.unsupported();
        }
    }

    private static ByteBuffer readBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    private static ByteBuffer readCollectionValue(ByteBuffer input, ProtocolVersion version) {
        int size;
        switch (version) {
            case V1:
            case V2:
                size = getUnsignedShort(input);
                break;
            case V3:
            case V4:
                size = input.getInt();
                break;
            default:
                throw version.unsupported();
        }
        return size < 0 ? null : readBytes(input, size);
    }

    private static int sizeOfValue(ByteBuffer value, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                int elemSize = value.remaining();
                if (elemSize > 65535)
                    throw new IllegalArgumentException("Native protocol version 2 supports only elements with size up to 65535 bytes - but element size is " + elemSize + " bytes");
                return 2 + elemSize;
            case V3:
            case V4:
                return value == null ? 4 : 4 + value.remaining();
            default:
                throw version.unsupported();
        }
    }

    static class StringCodec extends TypeCodec<String> {

        private final Charset charset;

        private StringCodec(Charset charset) {
            this.charset = charset;
        }

        @Override
        public String parse(String value) {
            if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
                throw new InvalidTypeException("text values must enclosed by a single quotes");

            return value.substring(1, value.length() - 1).replace("''", "'");
        }

        @Override
        public String format(String value) {
            return '\'' + replace(value, '\'', "''") + '\'';
        }

        // Simple method to replace a single character. String.replace is a bit too
        // inefficient (see JAVA-67)
        static String replace(String text, char search, String replacement) {
            if (text == null || text.isEmpty())
                return text;

            int nbMatch = 0;
            int start = -1;
            do {
                start = text.indexOf(search, start+1);
                if (start != -1)
                    ++nbMatch;
            } while (start != -1);

            if (nbMatch == 0)
                return text;

            int newLength = text.length() + nbMatch * (replacement.length() - 1);
            char[] result = new char[newLength];
            int newIdx = 0;
            for (int i = 0; i < text.length(); i++) {
                char c = text.charAt(i);
                if (c == search) {
                    for (int r = 0; r < replacement.length(); r++)
                        result[newIdx++] = replacement.charAt(r);
                } else {
                    result[newIdx++] = c;
                }
            }
            return new String(result);
        }

        @Override
        public ByteBuffer serialize(String value) {
            return ByteBuffer.wrap(value.getBytes(charset));
        }

        @Override
        public String deserialize(ByteBuffer bytes) {
            return new String(Bytes.getArray(bytes), charset);
        }
    }

    static class LongCodec extends TypeCodec<Long> {

        public static final LongCodec instance = new LongCodec();

        private LongCodec() {}

        @Override
        public Long parse(String value) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits long value from \"%s\"", value));
            }
        }

        @Override
        public String format(Long value) {
            return Long.toString(value);
        }

        @Override
        public ByteBuffer serialize(Long value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(long value) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putLong(0, value);
            return bb;
        }

        @Override
        public Long deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public long deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getLong(bytes.position());
        }
    }

    static class BytesCodec extends TypeCodec<ByteBuffer> {

        public static final BytesCodec instance = new BytesCodec();

        private BytesCodec() {}

        @Override
        public ByteBuffer parse(String value) {
            return Bytes.fromHexString(value);
        }

        @Override
        public String format(ByteBuffer value) {
            return Bytes.toHexString(value);
        }

        @Override
        public ByteBuffer serialize(ByteBuffer value) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes) {
            return bytes.duplicate();
        }
    }

    static class BooleanCodec extends TypeCodec<Boolean> {
        private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[]{1});
        private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[]{0});

        public static final BooleanCodec instance = new BooleanCodec();

        private BooleanCodec() {}

        @Override
        public Boolean parse(String value) {
            if (value.equalsIgnoreCase(Boolean.FALSE.toString()))
                return false;
            if (value.equalsIgnoreCase(Boolean.TRUE.toString()))
                return true;

            throw new InvalidTypeException(String.format("Cannot parse boolean value from \"%s\"", value));
        }

        @Override
        public String format(Boolean value) {
            return value ? "true" : "false";
        }

        @Override
        public ByteBuffer serialize(Boolean value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(boolean value) {
            return value ? TRUE.duplicate() : FALSE.duplicate();
        }

        @Override
        public Boolean deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public boolean deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 1)
                throw new InvalidTypeException("Invalid boolean value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position()) != 0;
        }
    }

    static class DecimalCodec extends TypeCodec<BigDecimal> {

        public static final DecimalCodec instance = new DecimalCodec();

        private DecimalCodec() {}

        @Override
        public BigDecimal parse(String value) {
            try {
                return new BigDecimal(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse decimal value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigDecimal value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigDecimal value) {
            BigInteger bi = value.unscaledValue();
            int scale = value.scale();
            byte[] bibytes = bi.toByteArray();

            ByteBuffer bytes = ByteBuffer.allocate(4 + bibytes.length);
            bytes.putInt(scale);
            bytes.put(bibytes);
            bytes.rewind();
            return bytes;
        }

        @Override
        public BigDecimal deserialize(ByteBuffer bytes) {
            if (bytes.remaining() < 4)
                throw new InvalidTypeException("Invalid decimal value, expecting at least 4 bytes but got " + bytes.remaining());

            bytes = bytes.duplicate();
            int scale = bytes.getInt();
            byte[] bibytes = new byte[bytes.remaining()];
            bytes.get(bibytes);

            BigInteger bi = new BigInteger(bibytes);
            return new BigDecimal(bi, scale);
        }
    }

    static class DoubleCodec extends TypeCodec<Double> {

        public static final DoubleCodec instance = new DoubleCodec();

        private DoubleCodec() {}

        @Override
        public Double parse(String value) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 64-bits double value from \"%s\"", value));
            }
        }

        @Override
        public String format(Double value) {
            return Double.toString(value);
        }

        @Override
        public ByteBuffer serialize(Double value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(double value) {
            ByteBuffer bb = ByteBuffer.allocate(8);
            bb.putDouble(0, value);
            return bb;
        }

        @Override
        public Double deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public double deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 8)
                throw new InvalidTypeException("Invalid 64-bits double value, expecting 8 bytes but got " + bytes.remaining());

            return bytes.getDouble(bytes.position());
        }
    }

    static class FloatCodec extends TypeCodec<Float> {

        public static final FloatCodec instance = new FloatCodec();

        private FloatCodec() {}

        @Override
        public Float parse(String value) {
            try {
                return Float.parseFloat(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits float value from \"%s\"", value));
            }
        }

        @Override
        public String format(Float value) {
            return Float.toString(value);
        }

        @Override
        public ByteBuffer serialize(Float value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(float value) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putFloat(0, value);
            return bb;
        }

        @Override
        public Float deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public float deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getFloat(bytes.position());
        }
    }

    static class InetCodec extends TypeCodec<InetAddress> {

        public static final InetCodec instance = new InetCodec();

        private InetCodec() {}

        @Override
        public InetAddress parse(String value) {
            value = value.trim();
            if (value.charAt(0) != '\'' || value.charAt(value.length() - 1) != '\'')
                throw new InvalidTypeException(String.format("inet values must be enclosed in single quotes (\"%s\")", value));
            try {
                return InetAddress.getByName(value.substring(1, value.length() - 1));
            } catch (Exception e) {
                throw new InvalidTypeException(String.format("Cannot parse inet value from \"%s\"", value));
            }
        }

        @Override
        public String format(InetAddress value) {
            return "'" + value.getHostAddress() + "'";
        }

        @Override
        public ByteBuffer serialize(InetAddress value) {
            return ByteBuffer.wrap(value.getAddress());
        }

        @Override
        public InetAddress deserialize(ByteBuffer bytes) {
            try {
                return InetAddress.getByAddress(Bytes.getArray(bytes));
            } catch (UnknownHostException e) {
                throw new InvalidTypeException("Invalid bytes for inet value, got " + bytes.remaining() + " bytes");
            }
        }
    }

    static class TinyIntCodec extends TypeCodec<Byte> {

        public static final TinyIntCodec instance = new TinyIntCodec();

        private TinyIntCodec() {}

        @Override
        public Byte parse(String value) {
            try {
                return Byte.parseByte(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 8-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Byte value) {
            return Byte.toString(value);
        }

        @Override
        public ByteBuffer serialize(Byte value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(byte value) {
            ByteBuffer bb = ByteBuffer.allocate(1);
            bb.put(0, value);
            return bb;
        }

        @Override
        public Byte deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public byte deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 1)
                throw new InvalidTypeException("Invalid 8-bits integer value, expecting 1 byte but got " + bytes.remaining());

            return bytes.get(bytes.position());
        }
    }

    static class SmallIntCodec extends TypeCodec<Short> {

        public static final SmallIntCodec instance = new SmallIntCodec();

        private SmallIntCodec() {}

        @Override
        public Short parse(String value) {
            try {
                return Short.parseShort(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 16-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Short value) {
            return Short.toString(value);
        }

        @Override
        public ByteBuffer serialize(Short value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(short value) {
            ByteBuffer bb = ByteBuffer.allocate(2);
            bb.putShort(0, value);
            return bb;
        }

        @Override
        public Short deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public Short deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 2)
                throw new InvalidTypeException("Invalid 16-bits integer value, expecting 2 bytes but got " + bytes.remaining());

            return bytes.getShort(bytes.position());
        }
    }

    static class IntCodec extends TypeCodec<Integer> {

        public static final IntCodec instance = new IntCodec();

        private IntCodec() {}

        @Override
        public Integer parse(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse 32-bits int value from \"%s\"", value));
            }
        }

        @Override
        public String format(Integer value) {
            return Integer.toString(value);
        }

        @Override
        public ByteBuffer serialize(Integer value) {
            return serializeNoBoxing(value);
        }

        public ByteBuffer serializeNoBoxing(int value) {
            ByteBuffer bb = ByteBuffer.allocate(4);
            bb.putInt(0, value);
            return bb;
        }

        @Override
        public Integer deserialize(ByteBuffer bytes) {
            return deserializeNoBoxing(bytes);
        }

        public int deserializeNoBoxing(ByteBuffer bytes) {
            if (bytes.remaining() != 4)
                throw new InvalidTypeException("Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());

            return bytes.getInt(bytes.position());
        }
    }

    static class DateCodec extends TypeCodec<Date> {

        private static final String[] iso8601Patterns = new String[] {
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mmZ",
            "yyyy-MM-dd HH:mm:ssZ",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd HH:mm:ss.SSSZ",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mmZ",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSS",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "yyyy-MM-dd",
            "yyyy-MM-ddZ"
        };

        public static final DateCodec instance = new DateCodec();
        private static final Pattern IS_LONG_PATTERN = Pattern.compile("^-?\\d+$");

        private DateCodec() {}

        /*
         * Copied and adapted from apache commons DateUtils.parseStrictly method (that is used Cassandra side
         * to parse date strings). It is copied here so as to not create a dependency on apache commons "just
         * for this".
         */
        private static Date parseDate(String str, final String[] parsePatterns) throws ParseException {
            SimpleDateFormat parser = new SimpleDateFormat();
            parser.setLenient(false);

            ParsePosition pos = new ParsePosition(0);
            for (String parsePattern : parsePatterns) {
                String pattern = parsePattern;

                parser.applyPattern(pattern);
                pos.setIndex(0);

                String str2 = str;
                Date date = parser.parse(str2, pos);
                if (date != null && pos.getIndex() == str2.length()) {
                    return date;
                }
            }
            throw new ParseException("Unable to parse the date: " + str, -1);
        }

        @Override
        public Date parse(String value) {
            if (IS_LONG_PATTERN.matcher(value).matches()) {
                try {
                    return new Date(Long.parseLong(value));
                } catch (NumberFormatException e) {
                    throw new InvalidTypeException(String.format("Cannot parse timestamp value from \"%s\"", value));
                }
            }

            try {
                return parseDate(value, iso8601Patterns);
            } catch (ParseException e) {
                throw new InvalidTypeException(String.format("Cannot parse date value from \"%s\"", value));
            }
        }

        @Override
        public String format(Date value) {
            return Long.toString(value.getTime());
        }

        @Override
        public ByteBuffer serialize(Date value) {
            return LongCodec.instance.serializeNoBoxing(value.getTime());
        }

        @Override
        public Date deserialize(ByteBuffer bytes) {
            return new Date(LongCodec.instance.deserializeNoBoxing(bytes));
        }
    }

    static class UUIDCodec extends TypeCodec<UUID> {

        public static final UUIDCodec instance = new UUIDCodec();

        protected UUIDCodec() {}

        @Override
        public UUID parse(String value) {
            try {
                return UUID.fromString(value);
            } catch (IllegalArgumentException e) {
                throw new InvalidTypeException(String.format("Cannot parse UUID value from \"%s\"", value));
            }
        }

        @Override
        public String format(UUID value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UUID value) {
            ByteBuffer bb = ByteBuffer.allocate(16);
            bb.putLong(0, value.getMostSignificantBits());
            bb.putLong(8, value.getLeastSignificantBits());
            return bb;
        }

        @Override
        public UUID deserialize(ByteBuffer bytes) {
            return new UUID(bytes.getLong(bytes.position() + 0), bytes.getLong(bytes.position() + 8));
        }
    }

    static class TimeUUIDCodec extends UUIDCodec {

        public static final TimeUUIDCodec instance = new TimeUUIDCodec();

        private TimeUUIDCodec() {}

        @Override
        public UUID parse(String value) {
            UUID id = super.parse(value);
            if (id.version() != 1)
                throw new InvalidTypeException(String.format("Cannot parse type 1 UUID value from \"%s\": represents a type %d UUID", value, id.version()));
            return id;
        }

        @Override
        public UUID deserialize(ByteBuffer bytes) {
            UUID id = super.deserialize(bytes);
            if (id.version() != 1)
                throw new InvalidTypeException(String.format("Error deserializing type 1 UUID: deserialized value %s represents a type %d UUID", id, id.version()));
            return id;
        }
    }

    static class BigIntegerCodec extends TypeCodec<BigInteger> {

        public static final BigIntegerCodec instance = new BigIntegerCodec();

        private BigIntegerCodec() {}

        @Override
        public BigInteger parse(String value) {
            try {
                return new BigInteger(value);
            } catch (NumberFormatException e) {
                throw new InvalidTypeException(String.format("Cannot parse varint value from \"%s\"", value));
            }
        }

        @Override
        public String format(BigInteger value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(BigInteger value) {
            return ByteBuffer.wrap(value.toByteArray());
        }

        @Override
        public BigInteger deserialize(ByteBuffer bytes) {
            return new BigInteger(Bytes.getArray(bytes));
        }
    }

    static class ListCodec<T> extends TypeCodec<List<T>> {

        private final TypeCodec<T> eltCodec;
        private final ProtocolVersion protocolVersion;

        public ListCodec(TypeCodec<T> eltCodec, ProtocolVersion protocolVersion) {
            this.eltCodec = eltCodec;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public List<T> parse(String value) {
            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '[')
                throw new InvalidTypeException(String.format("cannot parse list value from \"%s\", at character %d expecting '[' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == ']')
                return Collections.<T>emptyList();

            List<T> l = new ArrayList<T>();
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                l.add(eltCodec.parse(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == ']')
                    return l;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse list value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed list value \"%s\", missing closing ']'", value));
        }

        @Override
        public String format(List<T> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("[");
            for (int i = 0; i < value.size(); i++) {
                if (i != 0)
                    sb.append(", ");
                sb.append(eltCodec.format(value.get(i)));
            }
            sb.append("]");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(List<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            for (T elt : value)
                bbs.add(eltCodec.serialize(elt));

            return pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public List<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = readCollectionSize(input, protocolVersion);
                List<T> l = new ArrayList<T>(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer databb = readCollectionValue(input, protocolVersion);
                    l.add(eltCodec.deserialize(databb));
                }
                return l;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize list");
            }
        }
    }

    static class SetCodec<T> extends TypeCodec<Set<T>> {

        private final TypeCodec<T> eltCodec;
        private final ProtocolVersion protocolVersion;

        public SetCodec(TypeCodec<T> eltCodec, ProtocolVersion protocolVersion) {
            this.eltCodec = eltCodec;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public Set<T> parse(String value) {
            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(String.format("cannot parse set value from \"%s\", at character %d expecting '{' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}')
                return Collections.<T>emptySet();

            Set<T> s = new HashSet<T>();
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse set value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                s.add(eltCodec.parse(value.substring(idx, n)));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}')
                    return s;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse set value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed set value \"%s\", missing closing '}'", value));
        }

        @Override
        public String format(Set<T> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            int i = 0;
            for (T v : value) {
                if (i++ != 0)
                    sb.append(", ");
                sb.append(eltCodec.format(v));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Set<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            for (T elt : value)
                bbs.add(eltCodec.serialize(elt));

            return pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public Set<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = readCollectionSize(input, protocolVersion);
                Set<T> l = new LinkedHashSet<T>(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer databb = readCollectionValue(input, protocolVersion);
                    l.add(eltCodec.deserialize(databb));
                }
                return l;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a set");
            }
        }
    }

    static class MapCodec<K, V> extends TypeCodec<Map<K, V>> {

        private final TypeCodec<K> keyCodec;
        private final TypeCodec<V> valueCodec;
        private final ProtocolVersion protocolVersion;

        public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec, ProtocolVersion protocolVersion) {
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public Map<K, V> parse(String value) {
            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '{')
                throw new InvalidTypeException(String.format("cannot parse map value from \"%s\", at character %d expecting '{' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == '}')
                return Collections.<K, V>emptyMap();

            Map<K, V> m = new HashMap<K, V>();
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                K k = keyCodec.parse(value.substring(idx, n));
                idx = n;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx++) != ':')
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", at character %d expecting ':' but got '%c'", value, idx, value.charAt(idx)));
                idx = ParseUtils.skipSpaces(value, idx);

                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                V v = valueCodec.parse(value.substring(idx, n));
                idx = n;

                m.put(k, v);

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == '}')
                    return m;
                if (value.charAt(idx++) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse map value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));

                idx = ParseUtils.skipSpaces(value, idx);

            }
            throw new InvalidTypeException(String.format("Malformed map value \"%s\", missing closing '}'", value));
        }

        @Override
        public String format(Map<K, V> value) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            int i = 0;
            for (Map.Entry<K, V> e : value.entrySet()) {
                if (i++ != 0)
                    sb.append(", ");
                sb.append(keyCodec.format(e.getKey()));
                sb.append(":");
                sb.append(valueCodec.format(e.getValue()));
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public ByteBuffer serialize(Map<K, V> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
            for (Map.Entry<K, V> entry : value.entrySet()) {
                bbs.add(keyCodec.serialize(entry.getKey()));
                bbs.add(valueCodec.serialize(entry.getValue()));
            }
            return pack(bbs, value.size(), protocolVersion);
        }

        @Override
        public Map<K, V> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = readCollectionSize(input, protocolVersion);
                Map<K, V> m = new LinkedHashMap<K, V>(n);
                for (int i = 0; i < n; i++) {
                    ByteBuffer kbb = readCollectionValue(input, protocolVersion);
                    ByteBuffer vbb = readCollectionValue(input, protocolVersion);
                    m.put(keyCodec.deserialize(kbb), valueCodec.deserialize(vbb));
                }
                return m;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a map");
            }
        }
    }

    static class UDTCodec extends TypeCodec<UDTValue> {

        private final UserType definition;

        @SuppressWarnings("unused")
        private final ProtocolVersion protocolVersion;

        public UDTCodec(UserType definition, ProtocolVersion protocolVersion) {
            this.definition = definition;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public UDTValue parse(String value) {
            return definition.parseValue(value);
        }

        @Override
        public String format(UDTValue value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(UDTValue value) {
            int size = 0;
            for (ByteBuffer v : value.values)
                size += 4 + (v == null ? 0 : v.remaining());

            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : value.values) {
                if (bb == null) {
                    result.putInt(-1);
                } else {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer)result.flip();
        }

        @Override
        public UDTValue deserialize(ByteBuffer bytes) {
            ByteBuffer input = bytes.duplicate();
            UDTValue value = definition.newValue();

            int i = 0;
            while (input.hasRemaining() && i < value.values.length) {
                int n = input.getInt();
                value.values[i++] = n < 0 ? null : readBytes(input, n);
            }
            return value;
        }
    }

    static class TupleCodec extends TypeCodec<TupleValue> {

        private final TupleType type;
        private final ProtocolVersion protocolVersion;

        public TupleCodec(TupleType type, ProtocolVersion protocolVersion) {
            this.type = type;
            this.protocolVersion = protocolVersion;
        }

        @Override
        public TupleValue parse(String value) {
            TupleValue v = type.newValue();

            int idx = ParseUtils.skipSpaces(value, 0);
            if (value.charAt(idx++) != '(')
                throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting '(' but got '%c'", value, idx, value.charAt(idx)));

            idx = ParseUtils.skipSpaces(value, idx);

            if (value.charAt(idx) == ')')
                return v;

            int i = 0;
            while (idx < value.length()) {
                int n;
                try {
                    n = ParseUtils.skipCQLValue(value, idx);
                } catch (IllegalArgumentException e) {
                    throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", invalid CQL value at character %d", value, idx), e);
                }

                DataType dt = type.getComponentTypes().get(i);
                v.setBytesUnsafe(i, dt.serialize(dt.parse(value.substring(idx, n)), protocolVersion));
                idx = n;
                i += 1;

                idx = ParseUtils.skipSpaces(value, idx);
                if (value.charAt(idx) == ')')
                    return v;
                if (value.charAt(idx) != ',')
                    throw new InvalidTypeException(String.format("Cannot parse tuple value from \"%s\", at character %d expecting ',' but got '%c'", value, idx, value.charAt(idx)));
                ++idx; // skip ','

                idx = ParseUtils.skipSpaces(value, idx);
            }
            throw new InvalidTypeException(String.format("Malformed tuple value \"%s\", missing closing ')'", value));
        }

        @Override
        public String format(TupleValue value) {
            return value.toString();
        }

        @Override
        public ByteBuffer serialize(TupleValue value) {
            int size = 0;
            for (ByteBuffer v : value.values)
                size += 4 + (v == null ? 0 : v.remaining());

            ByteBuffer result = ByteBuffer.allocate(size);
            for (ByteBuffer bb : value.values) {
                if (bb == null) {
                    result.putInt(-1);
                } else {
                    result.putInt(bb.remaining());
                    result.put(bb.duplicate());
                }
            }
            return (ByteBuffer)result.flip();
        }

        @Override
        public TupleValue deserialize(ByteBuffer bytes) {
            ByteBuffer input = bytes.duplicate();
            TupleValue value = type.newValue();

            int i = 0;
            while (input.hasRemaining() && i < value.values.length) {
                int n = input.getInt();
                value.values[i++] = n < 0 ? null : readBytes(input, n);
            }
            return value;
        }
    }
}
