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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

/**
 * A set of utility methods to deal with type conversion,
 * serialization, and to create {@link TypeToken} instances.
 */
public class CodecUtils {

    public static <T> TypeToken<List<T>> listOf(Class<T> eltType) {
        return new TypeToken<List<T>>(){}.where(new TypeParameter<T>(){}, eltType);
    }

    public static <T> TypeToken<List<T>> listOf(TypeToken<T> eltType) {
        return new TypeToken<List<T>>(){}.where(new TypeParameter<T>(){}, eltType);
    }

    public static <T> TypeToken<Set<T>> setOf(Class<T> eltType) {
        return new TypeToken<Set<T>>(){}.where(new TypeParameter<T>(){}, eltType);
    }

    public static <T> TypeToken<Set<T>> setOf(TypeToken<T> eltType) {
        return new TypeToken<Set<T>>(){}.where(new TypeParameter<T>(){}, eltType);
    }

    public static <K, V> TypeToken<Map<K, V>> mapOf(Class<K> keyType, Class<V> valueType) {
        return new TypeToken<Map<K, V>>(){}
            .where(new TypeParameter<K>(){}, keyType)
            .where(new TypeParameter<V>(){}, valueType);
    }

    public static <K, V> TypeToken<Map<K, V>> mapOf(TypeToken<K> keyType, TypeToken<V> valueType) {
        return new TypeToken<Map<K, V>>(){}
            .where(new TypeParameter<K>(){}, keyType)
            .where(new TypeParameter<V>(){}, valueType);
    }

    public static ByteBuffer[] convert(Object[] values, CodecRegistry codecRegistry) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
                if (value == null) {
                    serializedValues[i] = null;
                } else {
                    if (value instanceof Token)
                        value = ((Token)value).getValue();
                    try {
                        TypeCodec<Object> codec = codecRegistry.codecFor(value);
                        serializedValues[i] = codec.serialize(value);
                    } catch (Exception e) {
                        // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                        throw new IllegalArgumentException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, value.getClass()), e);
                    }
                }
        }
        return serializedValues;
    }

    // Utility method for collections
    public static ByteBuffer pack(List<ByteBuffer> buffers, int elements, int size) {
        if (elements > 65535)
            throw new IllegalArgumentException("Native protocol version 2 supports up to 65535 elements in any collection - but collection contains " + elements + " elements");
        ByteBuffer result = ByteBuffer.allocate(2 + size);
        result.putShort((short)elements);
        for (ByteBuffer bb : buffers) {
            int elemSize = bb.remaining();
            if (elemSize > 65535)
                throw new IllegalArgumentException("Native protocol version 2 supports only elements with size up to 65535 bytes - but element size is " + elemSize + " bytes");
            result.putShort((short)elemSize);
            result.put(bb.duplicate());
        }
        return (ByteBuffer)result.flip();
    }

    // Utility method for collections
    public static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

    public static ByteBuffer compose(ByteBuffer... buffers) {
        if(buffers.length == 1) return buffers[0];

        int totalLength = 0;
        for (ByteBuffer bb : buffers)
            totalLength += 2 + bb.remaining() + 1;

        ByteBuffer out = ByteBuffer.allocate(totalLength);
        for (ByteBuffer buffer : buffers) {
            ByteBuffer bb = buffer.duplicate();
            putShortLength(out, bb.remaining());
            out.put(bb);
            out.put((byte)0);
        }
        out.flip();
        return out;
    }

    private static void putShortLength(ByteBuffer bb, int length) {
        bb.put((byte)((length >> 8) & 0xFF));
        bb.put((byte)(length & 0xFF));
    }
}
