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
package com.datastax.driver.extras.codecs;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.reflect.TypeToken;

import java.nio.ByteBuffer;

/**
 * An abstract {@link TypeCodec} that stores JAVA objects as serialized strings.
 * This can serve as a base for codecs dealing with XML or JSON formats.
 * <p/>
 * This codec can be seen as a convenience base class to help
 * implementing Java-to-XML or Java-to-JSON mappings, but it comes
 * with a performance penalty: each Java object is serialized
 * in two steps: first to a String, and then to a {@link ByteBuffer},
 * which means that each serialization actually incurs in two potentially
 * expensive operations being carried.
 * <p/>
 * If you are using an XML or JSON library that supports writing Java objects
 * directly to {@link ByteBuffer}s, consider writing your own codec instead of
 * using this one.
 *
 * @param <T> The Java type this codec serializes from and deserializes to.
 */
public abstract class ParsingCodec<T> extends TypeCodec<T> {

    private final TypeCodec<String> innerCodec;

    public ParsingCodec(Class<T> javaType) {
        this(TypeToken.of(javaType));
    }

    public ParsingCodec(TypeToken<T> javaType) {
        this(TypeCodec.varchar(), javaType);
    }

    public ParsingCodec(TypeCodec<String> innerCodec, Class<T> javaType) {
        this(innerCodec, TypeToken.of(javaType));
    }

    public ParsingCodec(TypeCodec<String> innerCodec, TypeToken<T> javaType) {
        super(innerCodec.getCqlType(), javaType);
        this.innerCodec = innerCodec;
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        return value == null ? null : innerCodec.serialize(toString(value), protocolVersion);
    }

    @Override
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        String value = innerCodec.deserialize(bytes, protocolVersion);
        return value == null ? null : fromString(value);
    }

    @Override
    public String format(T value) throws InvalidTypeException {
        return value == null ? null : innerCodec.format(toString(value));
    }

    @Override
    public T parse(String value) throws InvalidTypeException {
        return value == null || value.isEmpty() || value.equalsIgnoreCase("NULL") ? null : fromString(innerCodec.parse(value));
    }

    /**
     * Return the String representation of the given object;
     * no special CQL quoting should be applied here.
     * Null values should be accepted; in most cases, implementors
     * should return null for null inputs.
     *
     * @param value the value to convert into a string
     * @return the String representation of the given object
     */
    protected abstract String toString(T value);

    /**
     * Parse the given string into an object;
     * no special CQL unquoting should be applied here.
     * Null values should be accepted; in most cases, implementors
     * should return null for null inputs.
     *
     * @param value the string to parse
     * @return the parsed object.
     */
    protected abstract T fromString(String value);

}
