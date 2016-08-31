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

import com.google.common.reflect.TypeToken;

import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * An abstract TypeCodec that maps a Java Pojo to another Java object
 * that can in turn be serialized into a CQL type.
 * This can serve as a base for libraries dealing with Pojo mappings.
 *
 * @param <T> The outer Java type
 * @param <U> The inner Java type
 */
public abstract class MappingCodec<T, U> extends TypeCodec<T> {

    protected final TypeCodec<U> innerCodec;

    public MappingCodec(TypeCodec<U> innerCodec, Class<T> javaType) {
        this(innerCodec, TypeToken.of(javaType));
    }

    public MappingCodec(TypeCodec<U> innerCodec, TypeToken<T> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(serialize(value), protocolVersion);
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return deserialize(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public T parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equals(NULL) ? null : deserialize(innerCodec.parse(value));
    }

    @Override
    public String format(T value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(serialize(value));
    }

    protected abstract T deserialize(U value);

    protected abstract U serialize(T value);

}
