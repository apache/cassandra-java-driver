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
 * A codec that maps the CQL type {@code list<double>} to the Java type {@code double[]}.
 * <p/>
 * Note that this codec is designed for performance and converts CQL lists
 * <em>directly</em> to {@code double[]}, thus avoiding any unnecessary
 * boxing and unboxing of Java primitive {@code double} values;
 * it also instantiates arrays without the need for an intermediary
 * Java {@code List} object.
 */
public class DoubleArrayCodec extends AbstractPrimitiveArrayCodec<double[]> {

    public static final DoubleArrayCodec instance = new DoubleArrayCodec();

    public DoubleArrayCodec() {
        super(DataType.list(DataType.cdouble()), double[].class);
    }

    @Override
    protected int sizeOfComponentType() {
        return 8;
    }

    @Override
    protected void serializeElement(ByteBuffer output, double[] array, int index, ProtocolVersion protocolVersion) {
        output.putDouble(array[index]);
    }

    @Override
    protected void deserializeElement(ByteBuffer input, double[] array, int index, ProtocolVersion protocolVersion) {
        array[index] = input.getDouble();
    }

    @Override
    protected void formatElement(StringBuilder output, double[] array, int index) {
        output.append(array[index]);
    }

    @Override
    protected void parseElement(String input, double[] array, int index) {
        array[index] = Double.parseDouble(input);
    }

    @Override
    protected double[] newInstance(int size) {
        return new double[size];
    }

}
