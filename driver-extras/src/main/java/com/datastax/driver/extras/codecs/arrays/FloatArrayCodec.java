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
 * A codec that maps the CQL type {@code list<float>} to the Java type {@code float[]}.
 * <p/>
 * Note that this codec is designed for performance and converts CQL lists
 * <em>directly</em> to {@code float[]}, thus avoiding any unnecessary
 * boxing and unboxing of Java primitive {@code float} values;
 * it also instantiates arrays without the need for an intermediary
 * Java {@code List} object.
 */
public class FloatArrayCodec extends AbstractPrimitiveArrayCodec<float[]> {

    public static final FloatArrayCodec instance = new FloatArrayCodec();

    public FloatArrayCodec() {
        super(DataType.list(DataType.cfloat()), float[].class);
    }

    @Override
    protected int sizeOfComponentType() {
        return 4;
    }

    @Override
    protected void serializeElement(ByteBuffer output, float[] array, int index, ProtocolVersion protocolVersion) {
        output.putFloat(array[index]);
    }

    @Override
    protected void deserializeElement(ByteBuffer input, float[] array, int index, ProtocolVersion protocolVersion) {
        array[index] = input.getFloat();
    }

    @Override
    protected void formatElement(StringBuilder output, float[] array, int index) {
        output.append(array[index]);
    }

    @Override
    protected void parseElement(String input, float[] array, int index) {
        array[index] = Float.parseFloat(input);
    }

    @Override
    protected float[] newInstance(int size) {
        return new float[size];
    }

}
