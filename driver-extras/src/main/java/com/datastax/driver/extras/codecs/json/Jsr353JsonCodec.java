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

import javax.json.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A JSON codec that uses the
 * <a href="https://jcp.org/en/jsr/detail?id=353">Java API for JSON processing</a>
 * to perform serialization and deserialization of JSON structures.
 * <p/>
 * More specifically, this codec maps an arbitrary {@link JsonStructure} to
 * a CQL {@code varchar} column.
 * <p/>
 * This codec handles the Java type {@link JsonStructure}.
 * It is therefore required that values are set and retrieved using that exact Java type;
 * users should manually downcast to either {@link JsonObject} or {@link JsonArray},
 * as in the example below:
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
 * Note that at runtime, this codec requires the presence of both JSR-353 API
 * and a JSR-353-compatible runtime library, such as
 * <a href="https://jsonp.java.net/download.html">JSR-353's reference implementation</a>.
 * If you use Maven, this can be done by declaring the following dependencies in your project:
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
 *   <version>1.0.4</version>
 * </dependency>
 * }</pre>
 */
public class Jsr353JsonCodec extends TypeCodec<JsonStructure> {

    private final JsonReaderFactory readerFactory;

    private final JsonWriterFactory writerFactory;

    /**
     * Creates a new instance using a default configuration.
     */
    public Jsr353JsonCodec() {
        this(null);
    }

    /**
     * Creates a new instance using the provided configuration.
     *
     * @param config A map of provider-specific configuration properties. May be empty or {@code null}.
     */
    public Jsr353JsonCodec(Map<String, ?> config) {
        super(DataType.varchar(), JsonStructure.class);
        readerFactory = Json.createReaderFactory(config);
        writerFactory = Json.createWriterFactory(config);
    }

    @Override
    public ByteBuffer serialize(JsonStructure value, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (value == null)
            return null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            JsonWriter writer = writerFactory.createWriter(baos);
            writer.write(value);
            return ByteBuffer.wrap(baos.toByteArray());
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public JsonStructure deserialize(ByteBuffer bytes, ProtocolVersion protocolVersion) throws InvalidTypeException {
        if (bytes == null)
            return null;
        ByteArrayInputStream bais = new ByteArrayInputStream(Bytes.getArray(bytes));
        try {
            JsonReader reader = readerFactory.createReader(bais);
            return reader.read();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                bais.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
    }

    @Override
    public String format(JsonStructure value) throws InvalidTypeException {
        if (value == null)
            return "NULL";
        String json;
        StringWriter sw = new StringWriter();
        try {
            JsonWriter writer = writerFactory.createWriter(sw);
            writer.write(value);
            json = sw.toString();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            try {
                sw.close();
            } catch (IOException e) {
                // cannot happen
                throw new InvalidTypeException(e.getMessage(), e);
            }
        }
        return ParseUtils.quote(json);
    }

    @Override
    @SuppressWarnings("unchecked")
    public JsonStructure parse(String value) throws InvalidTypeException {
        if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
            return null;
        if (!ParseUtils.isQuoted(value))
            throw new InvalidTypeException("JSON strings must be enclosed by single quotes");
        String json = ParseUtils.unquote(value);
        StringReader sr = new StringReader(json);
        try {
            JsonReader reader = readerFactory.createReader(sr);
            return reader.read();
        } catch (JsonException e) {
            throw new InvalidTypeException(e.getMessage(), e);
        } finally {
            sr.close();
        }
    }

}
