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
package com.datastax.driver.extras.codecs.json;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.reflect.TypeToken;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A JSON codec that uses the <a href="http://wiki.fasterxml.com/JacksonHome">Jackson</a>
 * library to perform serialization and deserialization of JSON objects.
 * <p/>
 * This codec maps a single Java object to a single JSON structure at a time;
 * mapping of arrays or collections to root-level JSON arrays is not supported,
 * but such a codec can be easily crafted after this one.
 * <p/>
 * Note that this codec requires the presence of Jackson library at runtime.
 * If you use Maven, this can be done by declaring the following dependency in your project:
 * <p/>
 * <pre>{@code
 * <dependency>
 *   <groupId>com.fasterxml.jackson.core</groupId>
 *   <artifactId>jackson-databind</artifactId>
 *   <version>2.6.3</version>
 * </dependency>
 * }</pre>
 */
public class JacksonJsonCodec<T> extends TypeCodec<T> {

    private final ObjectMapper objectMapper;

    /**
     * Creates a new instance for the provided {@code javaClass},
     * using a default, newly-allocated {@link ObjectMapper}.
     *
     * @param javaClass the Java class this codec maps to.
     */
    public JacksonJsonCodec(Class<T> javaClass) {
        this(javaClass, new ObjectMapper());
    }

    /**
     * Creates a new instance for the provided {@code javaType},
     * using a default, newly-allocated {@link ObjectMapper}.
     *
     * @param javaType the Java type this codec maps to.
     */
    public JacksonJsonCodec(TypeToken<T> javaType) {
        this(javaType, new ObjectMapper());
    }

    /**
     * Creates a new instance for the provided {@code javaClass},
     * and using the provided {@link ObjectMapper}.
     *
     * @param javaClass the Java class this codec maps to.
     */
    public JacksonJsonCodec(Class<T> javaClass, ObjectMapper objectMapper) {
        this(TypeToken.of(javaClass), objectMapper);
    }

    /**
     * Creates a new instance for the provided {@code javaType},
     * and using the provided {@link ObjectMapper}.
     *
     * @param javaType the Java type this codec maps to.
     */
    public JacksonJsonCodec(TypeToken<T> javaType, ObjectMapper objectMapper) {
        super(DataType.varchar(), javaType);
        this.objectMapper = objectMapper;
    }

    @Override
    public ByteBuffer serialize(T value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        try {
            return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
        } catch (JsonProcessingException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null)
            return null;
        try {
            return (T) objectMapper.readValue(Bytes.getArray(bytes), toJacksonJavaType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    @Override
    public String format(T value) throws InvalidTypeException {
        if (value == null)
            return "NULL";
        String json;
        try {
            json = objectMapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
        return ParseUtils.quote(json);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        if (!ParseUtils.isQuoted(value))
            throw new InvalidTypeException("JSON strings must be enclosed by single quotes");
        String json = ParseUtils.unquote(value);
        try {
            return (T) objectMapper.readValue(json, toJacksonJavaType());
        } catch (IOException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        }
    }

    /**
     * This method acts as a bridge between Guava's {@link com.google.common.reflect.TypeToken TypeToken} API, which is used
     * by the driver, and Jackson's {@link JavaType} API.
     *
     * @return A {@link JavaType} instance corresponding to the codec's {@link #getJavaType() Java type}.
     */
    protected JavaType toJacksonJavaType() {
        return TypeFactory.defaultInstance().constructType(getJavaType().getType());
    }

}
