/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.extras.codecs.arrays;

import com.datastax.driver.core.CodecUtils;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.exceptions.InvalidTypeException;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Base class for all codecs dealing with Java primitive arrays.
 * This class provides a more efficient implementation of {@link #serialize(Object, ProtocolVersion)}
 * and {@link #deserialize(ByteBuffer, ProtocolVersion)} for primitive arrays.
 *
 * @param <T> The Java primitive array type this codec handles
 */
public abstract class AbstractPrimitiveArrayCodec<T> extends AbstractArrayCodec<T> {

    /**
     * @param cqlType   The CQL type. Must be a list type.
     * @param javaClass The Java type. Must be an array class.
     */
    public AbstractPrimitiveArrayCodec(DataType.CollectionType cqlType, Class<T> javaClass) {
        super(cqlType, javaClass);
        checkArgument(javaClass.getComponentType().isPrimitive(), "Expecting primitive array component type, got %s", javaClass.getComponentType());
    }

    @Override
    public ByteBuffer serialize(T array, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (array == null)
            return null;
        boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;
        // native method
        int length = Array.getLength(array);
        checkArgument(isProtocolV3OrAbove || length < 65536,
                "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                protocolVersion.toInt(), length);
        /*
         * Encoding of lists in the native protocol:
         * [size of list] [size of element 1][element1] [size of element 2][element2]...
         * Sizes are encoded on 2 bytes in protocol v1 and v2, 4 bytes otherwise.
         * (See native_protocol_v*.spec in https://github.com/apache/cassandra/blob/trunk/doc/)
         */
        // Number of bytes to encode sizes
        int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;
        // Number of bytes to encode each element (preceded by its size)
        int sizeOfElement = sizeOfSize + sizeOfComponentType();
        // Number of bytes to encode the whole collection (size of collection + all elements)
        int totalSize = sizeOfSize + length * sizeOfElement;
        ByteBuffer output = ByteBuffer.allocate(totalSize);
        CodecUtils.writeSize(output, length, protocolVersion);
        for (int i = 0; i < length; i++) {
            CodecUtils.writeSize(output, sizeOfComponentType(), protocolVersion);
            serializeElement(output, array, i, protocolVersion);
        }
        output.flip();
        return output;
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null || bytes.remaining() == 0)
            return newInstance(0);
        boolean isProtocolV3OrAbove = protocolVersion.compareTo(ProtocolVersion.V2) > 0;
        int sizeOfSize = isProtocolV3OrAbove ? 4 : 2;
        ByteBuffer input = bytes.duplicate();
        int size = CodecUtils.readSize(input, protocolVersion);
        T array = newInstance(size);
        for (int i = 0; i < size; i++) {
            // Skip size (we know it will always be 4 since the elements are ints)
            input.position(input.position() + sizeOfSize);
            deserializeElement(input, array, i, protocolVersion);
        }
        return array;
    }

    /**
     * Return the size in bytes of the array component type.
     *
     * @return the size in bytes of the array component type.
     */
    protected abstract int sizeOfComponentType();

    /**
     * Write the {@code index}th element of {@code array} to {@code output}.
     *
     * @param output          The ByteBuffer to write to.
     * @param array           The array to read from.
     * @param index           The element index.
     * @param protocolVersion The protocol version to use.
     */
    protected abstract void serializeElement(ByteBuffer output, T array, int index, ProtocolVersion protocolVersion);

    /**
     * Read the {@code index}th element of {@code array} from {@code input}.
     *
     * @param input           The ByteBuffer to read from.
     * @param array           The array to write to.
     * @param index           The element index.
     * @param protocolVersion The protocol version to use.
     */
    protected abstract void deserializeElement(ByteBuffer input, T array, int index, ProtocolVersion protocolVersion);

}
