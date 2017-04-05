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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;

import java.nio.ByteBuffer;

/**
 * A codec that maps the CQL type {@code list<long>} to the Java type {@code long[]}.
 * <p/>
 * Note that this codec is designed for performance and converts CQL lists
 * <em>directly</em> to {@code long[]}, thus avoiding any unnecessary
 * boxing and unboxing of Java primitive {@code long} values;
 * it also instantiates arrays without the need for an intermediary
 * Java {@code List} object.
 */
public class LongArrayCodec extends AbstractPrimitiveArrayCodec<long[]> {

    public static final LongArrayCodec instance = new LongArrayCodec();

    public LongArrayCodec() {
        super(DataType.list(DataType.bigint()), long[].class);
    }

    @Override
    protected int sizeOfComponentType() {
        return 8;
    }

    @Override
    protected void serializeElement(ByteBuffer output, long[] array, int index, ProtocolVersion protocolVersion) {
        output.putLong(array[index]);
    }

    @Override
    protected void deserializeElement(ByteBuffer input, long[] array, int index, ProtocolVersion protocolVersion) {
        array[index] = input.getLong();
    }

    @Override
    protected void formatElement(StringBuilder output, long[] array, int index) {
        output.append(array[index]);
    }

    @Override
    protected void parseElement(String input, long[] array, int index) {
        array[index] = Long.parseLong(input);
    }

    @Override
    protected long[] newInstance(int size) {
        return new long[size];
    }

}
