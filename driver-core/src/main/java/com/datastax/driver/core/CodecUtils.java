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

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A set of utility methods to deal with type conversion,
 * serialization, and to create {@link TypeToken} instances.
 * <p>
 * This class is public because it is accessed from {@link com.datastax.driver.core.querybuilder.BuiltStatement}
 * but its use by client code is not supported.
 */
public final class CodecUtils {

    private CodecUtils(){}

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

    /**
     * Utility method to serialize user-provided values.
     * <p>
     * This method is useful in situations where there is no metadata available and the underlying CQL
     * type for the values is not known.
     * <p>
     * This situation happens when a {@link SimpleStatement}
     * or a {@link com.datastax.driver.core.querybuilder.BuiltStatement} (Query Builder) contain values;
     * in these places, the driver has no way to determine the right CQL type to use.
     * <p>
     * This method performs a best-effort heuristic to guess which codec to use.
     * Note that this is not particularly efficient as the codec registry needs to iterate over
     * the registered codecs until it finds a suitable one.
     *
     * @param values The values to convert.
     * @param protocolVersion The protocol version to use.
     * @param codecRegistry The {@link CodecRegistry} to use.
     * @return The converted values.
     */
    public static ByteBuffer[] convert(Object[] values, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        ByteBuffer[] serializedValues = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            if (value == null) {
                // impossible to locate the right codec when object is null,
                // so forcing the result to null
                serializedValues[i] = null;
            } else {
                if (value instanceof Token) {
                    // bypass CodecRegistry for Token instances
                    serializedValues[i] = ((Token)value).serialize(protocolVersion);
                } else {
                    try {
                        TypeCodec<Object> codec = codecRegistry.codecFor(value);
                        serializedValues[i] = codec.serialize(value, protocolVersion);
                    } catch (Exception e) {
                        // Catch and rethrow to provide a more helpful error message (one that include which value is bad)
                        throw new InvalidTypeException(String.format("Value %d of type %s does not correspond to any CQL3 type", i, value.getClass()), e);
                    }
                }
            }
        }
        return serializedValues;
    }

    /**
     * Utility method to assemble different routing key components into a single {@link ByteBuffer}.
     * Mainly intended for statements that need to generate a routing key out of their current values.
     *
     * @param buffers the components of the routing key.
     * @return A ByteBuffer containing the serialized routing key
     */
    public static ByteBuffer compose(ByteBuffer... buffers) {
        if (buffers.length == 1)
            return buffers[0];

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

    /**
     * Utility method that "packs" together a list of {@link ByteBuffer}s containing
     * serialized collection elements.
     * Mainly intended for use with collection codecs when serializing collections.
     *
     * @param buffers the collection elements
     * @param elements the total number of elements
     * @param version the protocol version to use
     * @return The serialized collection
     */
    public static ByteBuffer pack(List<ByteBuffer> buffers, int elements, ProtocolVersion version) {
        int size = 0;
        for (ByteBuffer bb : buffers) {
            int elemSize = sizeOfValue(bb, version);
            size += elemSize;
        }
        ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize(version) + size);
        writeCollectionSize(result, elements, version);
        for (ByteBuffer bb : buffers)
            writeCollectionValue(result, bb, version);
        return (ByteBuffer)result.flip();
    }

    /**
     * Utility method that reads the collection size.
     * Mainly intended for collection codecs when deserializing CQL collections.
     * @param input
     * @param version
     * @return
     */
    public static int readCollectionSize(ByteBuffer input, ProtocolVersion version) {
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

    public static ByteBuffer readBytes(ByteBuffer bb, int length) {
        ByteBuffer copy = bb.duplicate();
        copy.limit(copy.position() + length);
        bb.position(bb.position() + length);
        return copy;
    }

    public static ByteBuffer readCollectionValue(ByteBuffer input, ProtocolVersion version) {
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


    private static void writeCollectionSize(ByteBuffer output, int elements, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                if (elements > 65535)
                    throw new IllegalArgumentException(String.format("Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements", version.toInt(), elements));
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

    private static int sizeOfCollectionSize(ProtocolVersion version) {
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

    private static int sizeOfValue(ByteBuffer value, ProtocolVersion version) {
        switch (version) {
            case V1:
            case V2:
                int elemSize = value.remaining();
                if (elemSize > 65535)
                    throw new IllegalArgumentException(String.format("Native protocol version %d supports only elements with size up to 65535 bytes - but element size is %d bytes", version.toInt(), elemSize));
                return 2 + elemSize;
            case V3:
            case V4:
                return value == null ? 4 : 4 + value.remaining();
            default:
                throw version.unsupported();
        }
    }

    private static int getUnsignedShort(ByteBuffer bb) {
        int length = (bb.get() & 0xFF) << 8;
        return length | (bb.get() & 0xFF);
    }

}
