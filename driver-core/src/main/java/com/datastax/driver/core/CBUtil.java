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

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.*;

/**
 * ByteBuf utility methods.
 */
abstract class CBUtil { // TODO rename

    private CBUtil() {
    }

    private static String readString(ByteBuf cb, int length) {
        try {
            String str = cb.toString(cb.readerIndex(), length, CharsetUtil.UTF_8);
            cb.readerIndex(cb.readerIndex() + length);
            return str;
        } catch (IllegalStateException e) {
            // That's the way netty encapsulate a CCE
            if (e.getCause() instanceof CharacterCodingException)
                throw new DriverInternalError("Cannot decode string as UTF8");
            else
                throw e;
        }
    }

    public static String readString(ByteBuf cb) {
        try {
            int length = cb.readUnsignedShort();
            return readString(cb, length);
        } catch (IndexOutOfBoundsException e) {
            throw new DriverInternalError("Not enough bytes to read an UTF8 serialized string preceded by it's 2 bytes length");
        }
    }

    public static void writeString(String str, ByteBuf cb) {
        byte[] bytes = str.getBytes(CharsetUtil.UTF_8);
        cb.writeShort(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfString(String str) {
        return 2 + encodedUTF8Length(str);
    }

    private static int encodedUTF8Length(String st) {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++) {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }

    public static String readLongString(ByteBuf cb) {
        try {
            int length = cb.readInt();
            return readString(cb, length);
        } catch (IndexOutOfBoundsException e) {
            throw new DriverInternalError("Not enough bytes to read an UTF8 serialized string preceded by it's 4 bytes length");
        }
    }

    public static void writeLongString(String str, ByteBuf cb) {
        byte[] bytes = str.getBytes(CharsetUtil.UTF_8);
        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static int sizeOfLongString(String str) {
        return 4 + str.getBytes(CharsetUtil.UTF_8).length;
    }

    public static byte[] readBytes(ByteBuf cb) {
        try {
            int length = cb.readUnsignedShort();
            byte[] bytes = new byte[length];
            cb.readBytes(bytes);
            return bytes;
        } catch (IndexOutOfBoundsException e) {
            throw new DriverInternalError("Not enough bytes to read a byte array preceded by it's 2 bytes length");
        }
    }

    public static void writeBytes(byte[] bytes, ByteBuf cb) {
        cb.writeShort(bytes.length);
        cb.writeBytes(bytes);
    }

    public static void writeBytes(ByteBuffer bytes, ByteBuf cb) {
        cb.writeShort(bytes.remaining());
        cb.writeBytes(bytes.duplicate());
    }

    public static int sizeOfBytes(byte[] bytes) {
        return 2 + bytes.length;
    }

    public static int sizeOfBytes(ByteBuffer bytes) {
        return 2 + bytes.remaining();
    }

    public static Map<String, ByteBuffer> readBytesMap(ByteBuf cb) {
        int length = cb.readUnsignedShort();
        ImmutableMap.Builder<String, ByteBuffer> builder = ImmutableMap.builder();
        for (int i = 0; i < length; i++) {
            String key = readString(cb);
            ByteBuffer value = readValue(cb);
            if (value == null)
                value = Statement.NULL_PAYLOAD_VALUE;
            builder.put(key, value);
        }
        return builder.build();
    }

    public static void writeBytesMap(Map<String, ByteBuffer> m, ByteBuf cb) {
        cb.writeShort(m.size());
        for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
            writeString(entry.getKey(), cb);
            ByteBuffer value = entry.getValue();
            if (value == Statement.NULL_PAYLOAD_VALUE)
                value = null;
            writeValue(value, cb);
        }
    }

    public static int sizeOfBytesMap(Map<String, ByteBuffer> m) {
        int size = 2;
        for (Map.Entry<String, ByteBuffer> entry : m.entrySet()) {
            size += sizeOfString(entry.getKey());
            size += sizeOfBytes(entry.getValue());
        }
        return size;
    }

    public static ConsistencyLevel readConsistencyLevel(ByteBuf cb) {
        return ConsistencyLevel.fromCode(cb.readUnsignedShort());
    }

    public static void writeConsistencyLevel(ConsistencyLevel consistency, ByteBuf cb) {
        cb.writeShort(consistency.code);
    }

    public static int sizeOfConsistencyLevel(ConsistencyLevel consistency) {
        return 2;
    }

    public static <T extends Enum<T>> T readEnumValue(Class<T> enumType, ByteBuf cb) {
        String value = CBUtil.readString(cb);
        try {
            return Enum.valueOf(enumType, value.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new DriverInternalError(String.format("Invalid value '%s' for %s", value, enumType.getSimpleName()));
        }
    }

    public static <T extends Enum<T>> void writeEnumValue(T enumValue, ByteBuf cb) {
        writeString(enumValue.toString(), cb);
    }

    public static <T extends Enum<T>> int sizeOfEnumValue(T enumValue) {
        return sizeOfString(enumValue.toString());
    }

    public static UUID readUUID(ByteBuf cb) {
        long msb = cb.readLong();
        long lsb = cb.readLong();
        return new UUID(msb, lsb);
    }

    public static void writeUUID(UUID uuid, ByteBuf cb) {
        cb.writeLong(uuid.getMostSignificantBits());
        cb.writeLong(uuid.getLeastSignificantBits());
    }

    public static int sizeOfUUID(UUID uuid) {
        return 16;
    }

    public static List<String> readStringList(ByteBuf cb) {
        int length = cb.readUnsignedShort();
        List<String> l = new ArrayList<String>(length);
        for (int i = 0; i < length; i++)
            l.add(readString(cb));
        return l;
    }

    public static void writeStringList(List<String> l, ByteBuf cb) {
        cb.writeShort(l.size());
        for (String str : l)
            writeString(str, cb);
    }

    public static int sizeOfStringList(List<String> l) {
        int size = 2;
        for (String str : l)
            size += sizeOfString(str);
        return size;
    }

    public static Map<String, String> readStringMap(ByteBuf cb) {
        int length = cb.readUnsignedShort();
        Map<String, String> m = new HashMap<String, String>(length);
        for (int i = 0; i < length; i++) {
            String k = readString(cb).toUpperCase();
            String v = readString(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringMap(Map<String, String> m, ByteBuf cb) {
        cb.writeShort(m.size());
        for (Map.Entry<String, String> entry : m.entrySet()) {
            writeString(entry.getKey(), cb);
            writeString(entry.getValue(), cb);
        }
    }

    public static int sizeOfStringMap(Map<String, String> m) {
        int size = 2;
        for (Map.Entry<String, String> entry : m.entrySet()) {
            size += sizeOfString(entry.getKey());
            size += sizeOfString(entry.getValue());
        }
        return size;
    }

    public static Map<String, List<String>> readStringToStringListMap(ByteBuf cb) {
        int length = cb.readUnsignedShort();
        Map<String, List<String>> m = new HashMap<String, List<String>>(length);
        for (int i = 0; i < length; i++) {
            String k = readString(cb).toUpperCase();
            List<String> v = readStringList(cb);
            m.put(k, v);
        }
        return m;
    }

    public static void writeStringToStringListMap(Map<String, List<String>> m, ByteBuf cb) {
        cb.writeShort(m.size());
        for (Map.Entry<String, List<String>> entry : m.entrySet()) {
            writeString(entry.getKey(), cb);
            writeStringList(entry.getValue(), cb);
        }
    }

    public static int sizeOfStringToStringListMap(Map<String, List<String>> m) {
        int size = 2;
        for (Map.Entry<String, List<String>> entry : m.entrySet()) {
            size += sizeOfString(entry.getKey());
            size += sizeOfStringList(entry.getValue());
        }
        return size;
    }

    public static ByteBuffer readValue(ByteBuf cb) {
        int length = cb.readInt();
        if (length < 0)
            return null;
        ByteBuf slice = cb.readSlice(length);

        return ByteBuffer.wrap(readRawBytes(slice));
    }

    public static void writeValue(byte[] bytes, ByteBuf cb) {
        if (bytes == null) {
            cb.writeInt(-1);
            return;
        }

        cb.writeInt(bytes.length);
        cb.writeBytes(bytes);
    }

    public static void writeValue(ByteBuffer bytes, ByteBuf cb) {
        if (bytes == null) {
            cb.writeInt(-1);
            return;
        }

        if (bytes == BoundStatement.UNSET) {
            cb.writeInt(-2);
            return;
        }

        cb.writeInt(bytes.remaining());
        cb.writeBytes(bytes.duplicate());
    }

    public static int sizeOfValue(byte[] bytes) {
        return 4 + (bytes == null ? 0 : bytes.length);
    }

    public static int sizeOfValue(ByteBuffer bytes) {
        return 4 + (bytes == null ? 0 : bytes.remaining());
    }

    public static List<ByteBuffer> readValueList(ByteBuf cb) {
        int size = cb.readUnsignedShort();
        if (size == 0)
            return Collections.<ByteBuffer>emptyList();

        List<ByteBuffer> l = new ArrayList<ByteBuffer>(size);
        for (int i = 0; i < size; i++)
            l.add(readValue(cb));
        return l;
    }

    public static void writeValueList(List<ByteBuffer> values, ByteBuf cb) {
        cb.writeShort(values.size());
        for (ByteBuffer value : values)
            CBUtil.writeValue(value, cb);
    }

    public static int sizeOfValueList(List<ByteBuffer> values) {
        int size = 2;
        for (ByteBuffer value : values)
            size += CBUtil.sizeOfValue(value);
        return size;
    }

    public static void writeNamedValueList(Map<String, ByteBuffer> namedValues, ByteBuf cb) {
        cb.writeShort(namedValues.size());
        for (Map.Entry<String, ByteBuffer> entry : namedValues.entrySet()) {
            CBUtil.writeString(entry.getKey(), cb);
            CBUtil.writeValue(entry.getValue(), cb);
        }
    }

    public static int sizeOfNamedValueList(Map<String, ByteBuffer> namedValues) {
        int size = 2;
        for (Map.Entry<String, ByteBuffer> entry : namedValues.entrySet()) {
            size += CBUtil.sizeOfString(entry.getKey());
            size += CBUtil.sizeOfValue(entry.getValue());
        }
        return size;
    }

    public static InetSocketAddress readInet(ByteBuf cb) {
        int addrSize = cb.readByte();
        byte[] address = new byte[addrSize];
        cb.readBytes(address);
        int port = cb.readInt();
        try {
            return new InetSocketAddress(InetAddress.getByAddress(address), port);
        } catch (UnknownHostException e) {
            throw new DriverInternalError(String.format("Invalid IP address (%d.%d.%d.%d) while deserializing inet address", address[0], address[1], address[2], address[3]));
        }
    }

    public static void writeInet(InetSocketAddress inet, ByteBuf cb) {
        byte[] address = inet.getAddress().getAddress();

        cb.writeByte(address.length);
        cb.writeBytes(address);
        cb.writeInt(inet.getPort());
    }

    public static int sizeOfInet(InetSocketAddress inet) {
        byte[] address = inet.getAddress().getAddress();
        return 1 + address.length + 4;
    }

    /*
     * Reads *all* readable bytes from {@code cb} and return them.
     * If {@code cb} is backed by an array, this will return the underlying array directly, without copy.
     */
    public static byte[] readRawBytes(ByteBuf cb) {
        if (cb.hasArray() && cb.readableBytes() == cb.array().length) {
            // Move the readerIndex just so we consistently consume the input
            cb.readerIndex(cb.writerIndex());
            return cb.array();
        }

        // Otherwise, just read the bytes in a new array
        byte[] bytes = new byte[cb.readableBytes()];
        cb.readBytes(bytes);
        return bytes;
    }
}
