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
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;

abstract class TypeCodec<T> {

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
        primitiveCodecs.put(DataType.Name.INT,       IntCodec.instance);
        primitiveCodecs.put(DataType.Name.TEXT,      StringCodec.utf8Instance);
        primitiveCodecs.put(DataType.Name.TIMESTAMP, DateCodec.instance);
        primitiveCodecs.put(DataType.Name.UUID,      UUIDCodec.instance);
        primitiveCodecs.put(DataType.Name.VARCHAR,   StringCodec.utf8Instance);
        primitiveCodecs.put(DataType.Name.VARINT,    BigIntegerCodec.instance);
        primitiveCodecs.put(DataType.Name.TIMEUUID,  TimeUUIDCodec.instance);
        primitiveCodecs.put(DataType.Name.CUSTOM,    BytesCodec.instance);
    }

    private static final Map<DataType.Name, TypeCodec<List<?>>> primitiveListsCodecs = new EnumMap<DataType.Name, TypeCodec<List<?>>>(DataType.Name.class);
    private static final Map<DataType.Name, TypeCodec<Set<?>>> primitiveSetsCodecs = new EnumMap<DataType.Name, TypeCodec<Set<?>>>(DataType.Name.class);
    private static final Map<DataType.Name, Map<DataType.Name, TypeCodec<Map<?, ?>>>> primitiveMapsCodecs = new EnumMap<DataType.Name, Map<DataType.Name, TypeCodec<Map<?, ?>>>>(DataType.Name.class);
    static {
        populatePrimitiveCollectionCodecs();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static void populatePrimitiveCollectionCodecs()
    {
        for (Map.Entry<DataType.Name, TypeCodec<?>> entry : primitiveCodecs.entrySet()) {
            DataType.Name type = entry.getKey();
            TypeCodec<?> codec = entry.getValue();
            primitiveListsCodecs.put(type, new ListCodec(codec));
            primitiveSetsCodecs.put(type, new SetCodec(codec));
            Map<DataType.Name, TypeCodec<Map<?, ?>>> valueMap = new EnumMap<DataType.Name, TypeCodec<Map<?, ?>>>(DataType.Name.class);
            for (Map.Entry<DataType.Name, TypeCodec<?>> valueEntry : primitiveCodecs.entrySet())
                valueMap.put(valueEntry.getKey(), new MapCodec(codec, valueEntry.getValue()));
            primitiveMapsCodecs.put(type, valueMap);
        }
    }

    private TypeCodec() {}

    public abstract T parse(String value);
    public abstract ByteBuffer serialize(T value);
    public abstract T deserialize(ByteBuffer bytes);

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<T> createFor(DataType.Name name) {
        assert !name.isCollection();
        return (TypeCodec<T>)primitiveCodecs.get(name);
    }

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<List<T>> listOf(DataType arg) {
        TypeCodec<List<?>> codec = primitiveListsCodecs.get(arg.getName());
        return codec != null
             ? (TypeCodec)codec
             : new ListCodec<T>(TypeCodec.<T>createFor(arg.getName()));
    }

    @SuppressWarnings("unchecked")
    static <T> TypeCodec<Set<T>> setOf(DataType arg) {
        TypeCodec<Set<?>> codec = primitiveSetsCodecs.get(arg.getName());
        return codec != null
             ? (TypeCodec)codec
             : new SetCodec<T>(TypeCodec.<T>createFor(arg.getName()));
    }

    @SuppressWarnings("unchecked")
    static <K, V> TypeCodec<Map<K, V>> mapOf(DataType keys, DataType values) {
        Map<DataType.Name, TypeCodec<Map<?, ?>>> valueCodecs = primitiveMapsCodecs.get(keys.getName());
        TypeCodec<Map<?, ?>> codec = valueCodecs == null ? null : valueCodecs.get(values.getName());
        return codec != null
             ? (TypeCodec)codec
             : new MapCodec<K, V>(TypeCodec.<K>createFor(keys.getName()), TypeCodec.<V>createFor(values.getName()));
    }

    /* This is ugly, but not sure how we can do much better/faster
     * Returns if it's doesn't correspond to a known type.
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

        return null;
    }

    // Utility method for collections
    private static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size) {
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers) {
            result.putShort((short)bb.remaining());
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

    // Utility method for collections
    private static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    static class StringCodec extends TypeCodec<String> {

        static final StringCodec utf8Instance = new StringCodec(Charset.forName("UTF-8"));
        static final StringCodec asciiInstance = new StringCodec(Charset.forName("US-ASCII"));

        private final Charset charset;

        private StringCodec(Charset charset) {
            this.charset = charset;
        }

        @Override
        public String parse(String value) {
            return value;
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
        public ByteBuffer serialize(ByteBuffer value) {
            return value.duplicate();
        }

        @Override
        public ByteBuffer deserialize(ByteBuffer bytes) {
            return bytes.duplicate();
        }
    }

    static class BooleanCodec extends TypeCodec<Boolean> {

        private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[] {1});
        private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[] {0});

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
            try {
                return InetAddress.getByName(value);
            } catch (Exception e) {
                throw new InvalidTypeException(String.format("Cannot parse inet value from \"%s\"", value));
            }
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

        public ListCodec(TypeCodec<T> eltCodec) {
            this.eltCodec = eltCodec;
        }

        @Override
        public List<T> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer serialize(List<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T elt : value) {
                ByteBuffer bb = eltCodec.serialize(elt);
                bbs.add(bb);
                size += 2 + bb.remaining();
            }
            return pack(bbs, value.size(), size);
        }

        @Override
        public List<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = getUnsignedShort(input);
                List<T> l = new ArrayList<T>(n);
                for (int i = 0; i < n; i++) {
                    int s = getUnsignedShort(input);
                    byte[] data = new byte[s];
                    input.get(data);
                    ByteBuffer databb = ByteBuffer.wrap(data);
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

        public SetCodec(TypeCodec<T> eltCodec) {
            this.eltCodec = eltCodec;
        }

        @Override
        public Set<T> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer serialize(Set<T> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(value.size());
            int size = 0;
            for (T elt : value) {
                ByteBuffer bb = eltCodec.serialize(elt);
                bbs.add(bb);
                size += 2 + bb.remaining();
            }
            return pack(bbs, value.size(), size);
        }

        @Override
        public Set<T> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = getUnsignedShort(input);
                Set<T> l = new LinkedHashSet<T>(n);
                for (int i = 0; i < n; i++) {
                    int s = getUnsignedShort(input);
                    byte[] data = new byte[s];
                    input.get(data);
                    ByteBuffer databb = ByteBuffer.wrap(data);
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

        public MapCodec(TypeCodec<K> keyCodec, TypeCodec<V> valueCodec) {
            this.keyCodec = keyCodec;
            this.valueCodec = valueCodec;
        }

        @Override
        public Map<K, V> parse(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer serialize(Map<K, V> value) {
            List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(2 * value.size());
            int size = 0;
            for (Map.Entry<K, V> entry : value.entrySet()) {
                ByteBuffer bbk = keyCodec.serialize(entry.getKey());
                ByteBuffer bbv = valueCodec.serialize(entry.getValue());
                bbs.add(bbk);
                bbs.add(bbv);
                size += 4 + bbk.remaining() + bbv.remaining();
            }
            return pack(bbs, value.size(), size);
        }

        @Override
        public Map<K, V> deserialize(ByteBuffer bytes) {
            try {
                ByteBuffer input = bytes.duplicate();
                int n = getUnsignedShort(input);
                Map<K, V> m = new LinkedHashMap<K, V>(n);
                for (int i = 0; i < n; i++) {
                    int sk = getUnsignedShort(input);
                    byte[] datak = new byte[sk];
                    input.get(datak);
                    ByteBuffer kbb = ByteBuffer.wrap(datak);

                    int sv = getUnsignedShort(input);
                    byte[] datav = new byte[sv];
                    input.get(datav);
                    ByteBuffer vbb = ByteBuffer.wrap(datav);

                    m.put(keyCodec.deserialize(kbb), valueCodec.deserialize(vbb));
                }
                return m;
            } catch (BufferUnderflowException e) {
                throw new InvalidTypeException("Not enough bytes to deserialize a map");
            }
        }
    }
}
