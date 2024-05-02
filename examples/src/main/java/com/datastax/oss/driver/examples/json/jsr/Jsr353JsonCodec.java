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
package com.datastax.oss.driver.examples.json.jsr;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.Strings;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.Map;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonStructure;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;

/**
 * A JSON codec that uses the <a href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON
 * processing</a> to perform serialization and deserialization of JSON structures.
 *
 * <p>More specifically, this codec maps an arbitrary {@link JsonStructure} to a CQL {@code varchar}
 * column.
 *
 * <p>This codec handles the Java type {@link JsonStructure}. It is therefore required that values
 * are set and retrieved using that exact Java type; users should manually downcast to either {@link
 * JsonObject} or {@link JsonArray}, as in the example below:
 *
 * <pre>{@code
 * // setting values
 * JsonObject myObject = ...
 * PreparedStatement ps = ...
 * // set values using JsonStructure as target Java type
 * BoundStatement bs = ps.bind().set(1, myObject, JsonStructure.class);
 *
 * // retrieving values
 * Row row = session.execute(bs).one();
 * // use JsonStructure as target Java type to retrieve values
 * JsonStructure json = row.get(0, JsonStructure.class);
 * if (json instanceof JsonObject) {
 *     myObject = (JsonObject) json;
 *     ...
 * }
 * }</pre>
 *
 * <p>Note that at runtime, this codec requires the presence of both JSR-353 API and a
 * JSR-353-compatible runtime library, such as <a
 * href="https://jsonp.java.net/download.html">JSR-353's reference implementation</a>. If you use
 * Maven, this can be done by declaring the following dependencies in your project:
 *
 * <pre>{@code
 * <dependency>
 *   <groupId>javax.json</groupId>
 *   <artifactId>javax.json-api</artifactId>
 *   <version>1.0</version>
 * </dependency>
 *
 * <dependency>
 *   <groupId>org.glassfish</groupId>
 *   <artifactId>javax.json</artifactId>
 *   <version>1.1.4</version>
 * </dependency>
 * }</pre>
 */
public class Jsr353JsonCodec implements TypeCodec<JsonStructure> {

  private final JsonReaderFactory readerFactory;

  private final JsonWriterFactory writerFactory;

  /** Creates a new instance using a default configuration. */
  public Jsr353JsonCodec() {
    this(null);
  }

  /**
   * Creates a new instance using the provided configuration.
   *
   * @param config A map of provider-specific configuration properties. May be empty or {@code
   *     null}.
   */
  public Jsr353JsonCodec(@Nullable Map<String, ?> config) {
    readerFactory = Json.createReaderFactory(config);
    writerFactory = Json.createWriterFactory(config);
  }

  @NonNull
  @Override
  public GenericType<JsonStructure> getJavaType() {
    return GenericType.of(JsonStructure.class);
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TEXT;
  }

  @Nullable
  @Override
  public ByteBuffer encode(
      @Nullable JsonStructure value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      JsonWriter writer = writerFactory.createWriter(baos);
      writer.write(value);
      return ByteBuffer.wrap(baos.toByteArray());
    } catch (JsonException | IOException e) {
      throw new IllegalArgumentException("Failed to encode value as JSON", e);
    }
  }

  @Nullable
  @Override
  public JsonStructure decode(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    }
    try (ByteArrayInputStream bais = new ByteArrayInputStream(Bytes.getArray(bytes))) {
      JsonReader reader = readerFactory.createReader(bais);
      return reader.read();
    } catch (JsonException | IOException e) {
      throw new IllegalArgumentException("Failed to decode JSON value", e);
    }
  }

  @NonNull
  @Override
  public String format(@Nullable JsonStructure value) {
    if (value == null) {
      return "NULL";
    }
    String json;
    try (StringWriter sw = new StringWriter()) {
      JsonWriter writer = writerFactory.createWriter(sw);
      writer.write(value);
      json = sw.toString();
    } catch (JsonException | IOException e) {
      throw new IllegalArgumentException("Failed to format value as JSON", e);
    }
    return Strings.quote(json);
  }

  @Nullable
  @Override
  public JsonStructure parse(String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }
    if (!Strings.isQuoted(value)) {
      throw new IllegalArgumentException("JSON strings must be enclosed by single quotes");
    }
    String json = Strings.unquote(value);
    try (StringReader sr = new StringReader(json)) {
      JsonReader reader = readerFactory.createReader(sr);
      return reader.read();
    } catch (JsonException e) {
      throw new IllegalArgumentException("Failed to parse value as JSON", e);
    }
  }
}
