/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.examples.json.codecs;

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

/**
 * A JSON codec that uses the <a href="http://wiki.fasterxml.com/JacksonHome">Jackson</a> library to
 * perform serialization and deserialization of JSON objects.
 *
 * <p>This codec maps a single Java object to a single JSON structure at a time; mapping of arrays
 * or collections to root-level JSON arrays is not supported, but such a codec can be easily crafted
 * after this one.
 *
 * <p>Note that this codec requires the presence of Jackson library at runtime. If you use Maven,
 * this can be done by declaring the following dependency in your project:
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>com.fasterxml.jackson.core</groupId>
 *   <artifactId>jackson-databind</artifactId>
 *   <version>2.9.8</version>
 * </dependency>
 * }</pre>
 */
public class JacksonJsonCodec<T> implements TypeCodec<T> {

  private final ObjectMapper objectMapper;
  private final GenericType<T> javaType;

  /**
   * Creates a new instance for the provided {@code javaClass}, using a default, newly-allocated
   * {@link ObjectMapper}.
   *
   * @param javaClass the Java class this codec maps to.
   */
  public JacksonJsonCodec(Class<T> javaClass) {
    this(javaClass, new ObjectMapper());
  }

  /**
   * Creates a new instance for the provided {@code javaClass}, and using the provided {@link
   * ObjectMapper}.
   *
   * @param javaClass the Java class this codec maps to.
   */
  public JacksonJsonCodec(Class<T> javaClass, ObjectMapper objectMapper) {
    this.javaType = GenericType.of(javaClass);
    this.objectMapper = objectMapper;
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
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @Nullable
  @Override
  public T decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    }
    try {
      return objectMapper.readValue(Bytes.getArray(bytes), toJacksonJavaType());
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  @NonNull
  @Override
  public String format(T value) {
    if (value == null) {
      return "NULL";
    }
    String json;
    try {
      json = objectMapper.writeValueAsString(value);
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    return Strings.quote(json);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public T parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    if (!Strings.isQuoted(value)) {
      throw new IllegalArgumentException("JSON strings must be enclosed by single quotes");
    }
    String json = Strings.unquote(value);
    try {
      return (T) objectMapper.readValue(json, toJacksonJavaType());
    } catch (IOException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
  }

  /**
   * This method acts as a bridge between the driver's {@link
   * com.datastax.oss.driver.api.core.type.reflect.GenericType GenericType} API and Jackson's {@link
   * JavaType} API.
   *
   * @return A {@link JavaType} instance corresponding to the codec's {@link #getJavaType() Java
   *     type}.
   */
  private JavaType toJacksonJavaType() {
    return TypeFactory.defaultInstance().constructType(getJavaType().getType());
  }
}
