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
 * A codec that serializes {@link Enum} instances as CQL {@link DataType#varchar() varchar}s
 * representing their programmatic names as returned by {@link Enum#name()}.
 * <p>
 * This codec is automatically used by the driver to create codecs for {@link Enum}
 * instances on the fly.
 *
 * @param <E> The Enum class this codec serializes from and deserializes to.
 */
public class EnumStringCodec<E extends Enum<E>> extends TypeCodec<E> {

    private final Class<E> enumClass;

    private final TypeCodec<String> innerCodec;

    public EnumStringCodec(Class<E> enumClass) {
        this(TypeCodec.varcharCodec(), enumClass);
    }

    public EnumStringCodec(TypeCodec<String> innerCodec, Class<E> enumClass) {
        super(innerCodec.getCqlType(), enumClass);
        this.innerCodec = innerCodec;
        this.enumClass = enumClass;
    }

    @Override
    public ByteBuffer serialize(E value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return value == null ? null : innerCodec.serialize(value.name(), protocolVersion);
    }

    @Override
    public E deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        String value = innerCodec.deserialize(bytes, protocolVersion);
        return value == null ? null : Enum.valueOf(enumClass, value);
    }

    @Override
    public String format(E value) throws InvalidTypeException {
        if (value == null)
            return NULL;
        return innerCodec.format(value.name());
    }

    @Override
    public E parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(NULL) ? null : Enum.valueOf(enumClass, innerCodec.parse(value));
    }

}
