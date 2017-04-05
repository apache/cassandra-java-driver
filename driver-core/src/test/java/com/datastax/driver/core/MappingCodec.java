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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;

import java.nio.ByteBuffer;

/**
 * This class is a copy of MappingCodec declared in the extras module,
 * to avoid circular dependencies between Maven modules.
 */
public abstract class MappingCodec<O, I> extends TypeCodec<O> {

    protected final TypeCodec<I> innerCodec;

    public MappingCodec(TypeCodec<I> innerCodec, Class<O> javaType) {
        this(innerCodec, TypeToken.of(javaType));
    }

    public MappingCodec(TypeCodec<I> innerCodec, TypeToken<O> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
    }

    @Override
    public ByteBuffer serialize(O value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return innerCodec.serialize(serialize(value), protocolVersion);
    }

    @Override
    public O deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return deserialize(innerCodec.deserialize(bytes, protocolVersion));
    }

    @Override
    public O parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : deserialize(innerCodec.parse(value));
    }

    @Override
    public String format(O value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(serialize(value));
    }

    protected abstract O deserialize(I value);

    protected abstract I serialize(O value);

}
