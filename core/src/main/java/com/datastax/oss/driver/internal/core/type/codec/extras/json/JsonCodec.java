/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.type.codec.extras.json;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A JSON codec that maps arbitrary Java objects to JSON strings stored as CQL type {@code text},
 * using the Jackson library to perform serialization and deserialization of JSON objects.
 *
 * <p>Note that this codec requires the presence of Jackson library at runtime. If you use Maven,
 * this can be done by declaring the following dependency in your project:
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>com.fasterxml.jackson.core</groupId>
 *   <artifactId>jackson-databind</artifactId>
 *   <version>LATEST</version>
 * </dependency>
 * }</pre>
 *
 * @see <a href="http://wiki.fasterxml.com/JacksonHome">Jackson JSON Library</a>
 * @param <T> The Java type that this codec serializes from and deserializes to, from JSON strings.
 */
public class JsonCodec<T> implements TypeCodec<T> {

  private final ObjectMapper objectMapper;
  private final GenericType<T> javaType;
  private final JavaType jacksonJavaType;

  /**
   * Creates a new instance for the provided {@code javaClass}, using a default, newly-allocated
   * {@link ObjectMapper}.
   *
   * <p>The codec created with this constructor can handle all primitive CQL types as well as
   * collections thereof, however it cannot handle tuples and user-defined types; if you need
   * support for such CQL types, you need to create your own {@link ObjectMapper} and use the
   * {@linkplain #JsonCodec(Class, ObjectMapper) two-arg constructor} instead.
   *
   * @param javaClass the Java class this codec maps to.
   */
  public JsonCodec(@NonNull Class<T> javaClass) {
    this(GenericType.of(Objects.requireNonNull(javaClass, "javaClass cannot be null")));
  }

  /**
   * Creates a new instance for the provided {@code javaType}, using a default, newly-allocated
   * {@link ObjectMapper}.
   *
   * <p>The codec created with this constructor can handle all primitive CQL types as well as
   * collections thereof, however it cannot handle tuples and user-defined types; if you need
   * support for such CQL types, you need to create your own {@link ObjectMapper} and use the
   * {@linkplain #JsonCodec(GenericType, ObjectMapper) two-arg constructor} instead.
   *
   * @param javaType the Java type this codec maps to.
   */
  public JsonCodec(@NonNull GenericType<T> javaType) {
    this(javaType, new ObjectMapper());
  }

  /**
   * Creates a new instance for the provided {@code javaClass}, and using the provided {@link
   * ObjectMapper}.
   *
   * @param javaClass the Java class this codec maps to.
   * @param objectMapper the {@link ObjectMapper} instance to use.
   */
  public JsonCodec(@NonNull Class<T> javaClass, @NonNull ObjectMapper objectMapper) {
    this(
        GenericType.of(Objects.requireNonNull(javaClass, "javaClass cannot be null")),
        objectMapper);
  }

  /**
   * Creates a new instance for the provided {@code javaType}, and using the provided {@link
   * ObjectMapper}.
   *
   * @param javaType the Java type this codec maps to.
   * @param objectMapper the {@link ObjectMapper} instance to use.
   */
  public JsonCodec(@NonNull GenericType<T> javaType, @NonNull ObjectMapper objectMapper) {
    this.javaType = Objects.requireNonNull(javaType, "javaType cannot be null");
    this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper cannot be null");
    this.jacksonJavaType = TypeFactory.defaultInstance().constructType(javaType.getType());
  }

  @NonNull
  @Override
  public GenericType<T> getJavaType() {
    return javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TEXT;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable T value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    try {
      return ByteBuffer.wrap(objectMapper.writeValueAsBytes(value));
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to encode value as JSON", e);
    }
  }

  @Nullable
  @Override
  public T decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    }
    try {
      return objectMapper.readValue(Bytes.getArray(bytes), jacksonJavaType);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to decode JSON value", e);
    }
  }

  @NonNull
  @Override
  public String format(@Nullable T value) {
    if (value == null) {
      return "NULL";
    }
    String json;
    try {
      json = objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Failed to format value as JSON", e);
    }
    return Strings.quote(json);
  }

  @Nullable
  @Override
  public T parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    if (!Strings.isQuoted(value)) {
      throw new IllegalArgumentException("JSON strings must be enclosed by single quotes");
    }
    String json = Strings.unquote(value);
    try {
      return objectMapper.readValue(json, jacksonJavaType);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse value as JSON", e);
    }
  }
}
