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

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * A codec that serializes {@link Enum} instances as CQL {@link DataType#cint() int}s
 * representing their ordinal values as returned by {@link Enum#ordinal()}.
 * <p>
 * Note that, by default, the driver uses {@link EnumStringCodec} to serialize {@link Enum} instances;
 * to force the use of this codec instead for a specific {@link Enum} instance,
 * the appropriate {@link EnumIntCodec} instance must be explicitly
 * {@link CodecRegistry#register(TypeCodec) registered}.
 * <p>
 * <strong>Note that this codec relies on the enum constants declaration order;
 * it is therefore vital that this order remains immutable.</strong>
 *
 * @param <E> The Enum class this codec serializes from and deserializes to.
 */
public class EnumIntCodec<E extends Enum<E>> extends TypeCodec<E> {

    private final E[] enumConstants;

    private final TypeCodec<Integer> innerCodec;

    public EnumIntCodec(Class<E> enumClass) {
        this(TypeCodec.intCodec(), enumClass);
    }

    public EnumIntCodec(TypeCodec<Integer> innerCodec, Class<E> enumClass) {
        super(innerCodec.getCqlType(), enumClass);
        this.enumConstants = enumClass.getEnumConstants();
        this.innerCodec = innerCodec;
    }

    @Override
    public ByteBuffer serialize(E value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return value == null ? null : innerCodec.serialize(value.ordinal(), protocolVersion);
    }

    @Override
    public E deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        Integer value = innerCodec.deserialize(bytes, protocolVersion);
        return value == null ? null : enumConstants[value];
    }

    @Override
    public String format(E value) throws InvalidTypeException {
        if (value == null)
            return NULL;
        return innerCodec.format(value.ordinal());
    }

    @Override
    public E parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(NULL) ? null : enumConstants[innerCodec.parse(value)];
    }

}
