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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParseException;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.Version;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.JsonSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

public class GraphSON1SerdeTP {

  //////////////////////// DESERIALIZERS ////////////////////////

  /**
   * Default deserializer used by the driver for {@link InetAddress} instances. The actual subclass
   * returned by this deserializer depends on the type of address: {@link Inet4Address IPV4} or
   * {@link Inet6Address IPV6}.
   */
  static class DefaultInetAddressDeserializer<T extends InetAddress> extends StdDeserializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> inetClass;

    DefaultInetAddressDeserializer(Class<T> inetClass) {
      super(inetClass);
      this.inetClass = inetClass;
    }

    @Override
    public T deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
      String ip = parser.readValueAs(String.class);
      try {
        InetAddress inet = InetAddresses.forString(ip);
        return inetClass.cast(inet);
      } catch (ClassCastException e) {
        throw new JsonParseException(
            parser,
            String.format("Inet address cannot be cast to %s: %s", inetClass.getSimpleName(), ip),
            e);
      } catch (IllegalArgumentException e) {
        throw new JsonParseException(parser, String.format("Expected inet address, got %s", ip), e);
      }
    }
  }

  /**
   * Default deserializer used by the driver for geospatial types. It deserializes such types into
   * {@link Geometry} instances. The actual subclass depends on the type being deserialized.
   */
  static class DefaultGeometryDeserializer<T extends Geometry> extends StdDeserializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> geometryClass;

    DefaultGeometryDeserializer(Class<T> geometryClass) {
      super(geometryClass);
      this.geometryClass = geometryClass;
    }

    @Override
    public T deserialize(JsonParser parser, DeserializationContext ctx) throws IOException {
      String wkt = parser.readValueAs(String.class);
      Geometry geometry;
      if (wkt.startsWith("POINT")) geometry = Point.fromWellKnownText(wkt);
      else if (wkt.startsWith("LINESTRING")) geometry = LineString.fromWellKnownText(wkt);
      else if (wkt.startsWith("POLYGON")) geometry = Polygon.fromWellKnownText(wkt);
      else throw new JsonParseException(parser, "Unknown geometry type: " + wkt);
      return geometryClass.cast(geometry);
    }
  }

  /** Base class for serializing the {@code java.time.*} types to ISO-8061 formats. */
  abstract static class AbstractJavaTimeSerializer<T> extends StdSerializer<T> {

    private static final long serialVersionUID = 1L;

    AbstractJavaTimeSerializer(final Class<T> clazz) {
      super(clazz);
    }

    @Override
    public void serialize(
        final T value, final JsonGenerator gen, final SerializerProvider serializerProvider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  /** Base class for deserializing the {@code java.time.*} types from ISO-8061 formats. */
  abstract static class AbstractJavaTimeJacksonDeserializer<T> extends StdDeserializer<T> {

    private static final long serialVersionUID = 1L;

    AbstractJavaTimeJacksonDeserializer(final Class<T> clazz) {
      super(clazz);
    }

    abstract T parse(final String val);

    @Override
    public T deserialize(
        final JsonParser jsonParser, final DeserializationContext deserializationContext)
        throws IOException {
      return parse(jsonParser.getText());
    }
  }

  static final class DurationJacksonSerializer
      extends AbstractJavaTimeSerializer<java.time.Duration> {

    private static final long serialVersionUID = 1L;

    DurationJacksonSerializer() {
      super(java.time.Duration.class);
    }
  }

  static final class DurationJacksonDeserializer
      extends AbstractJavaTimeJacksonDeserializer<java.time.Duration> {

    private static final long serialVersionUID = 1L;

    DurationJacksonDeserializer() {
      super(java.time.Duration.class);
    }

    @Override
    public java.time.Duration parse(final String val) {
      return java.time.Duration.parse(val);
    }
  }

  static final class InstantJacksonSerializer
      extends AbstractJavaTimeSerializer<java.time.Instant> {

    private static final long serialVersionUID = 1L;

    InstantJacksonSerializer() {
      super(java.time.Instant.class);
    }
  }

  static final class InstantJacksonDeserializer
      extends AbstractJavaTimeJacksonDeserializer<java.time.Instant> {

    private static final long serialVersionUID = 1L;

    InstantJacksonDeserializer() {
      super(java.time.Instant.class);
    }

    @Override
    public java.time.Instant parse(final String val) {
      return java.time.Instant.parse(val);
    }
  }

  static final class LocalDateJacksonSerializer
      extends AbstractJavaTimeSerializer<java.time.LocalDate> {

    private static final long serialVersionUID = 1L;

    LocalDateJacksonSerializer() {
      super(java.time.LocalDate.class);
    }
  }

  static final class LocalDateJacksonDeserializer
      extends AbstractJavaTimeJacksonDeserializer<java.time.LocalDate> {

    private static final long serialVersionUID = 1L;

    LocalDateJacksonDeserializer() {
      super(java.time.LocalDate.class);
    }

    @Override
    public java.time.LocalDate parse(final String val) {
      return java.time.LocalDate.parse(val);
    }
  }

  static final class LocalTimeJacksonSerializer
      extends AbstractJavaTimeSerializer<java.time.LocalTime> {

    private static final long serialVersionUID = 1L;

    LocalTimeJacksonSerializer() {
      super(java.time.LocalTime.class);
    }
  }

  static final class LocalTimeJacksonDeserializer
      extends AbstractJavaTimeJacksonDeserializer<java.time.LocalTime> {

    private static final long serialVersionUID = 1L;

    LocalTimeJacksonDeserializer() {
      super(java.time.LocalTime.class);
    }

    @Override
    public java.time.LocalTime parse(final String val) {
      return java.time.LocalTime.parse(val);
    }
  }

  //////////////////////// SERIALIZERS ////////////////////////

  /** Default serializer used by the driver for {@link LegacyGraphNode} instances. */
  static class DefaultGraphNodeSerializer extends StdSerializer<LegacyGraphNode> {

    private static final long serialVersionUID = 1L;

    DefaultGraphNodeSerializer() {
      super(LegacyGraphNode.class);
    }

    @Override
    public void serialize(
        LegacyGraphNode value, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeTree(value.getDelegate());
    }
  }

  /**
   * Default serializer used by the driver for geospatial types. It serializes {@link Geometry}
   * instances into their Well-Known Text (WKT) equivalent.
   */
  static class DefaultGeometrySerializer extends StdSerializer<Geometry> {

    private static final long serialVersionUID = 1L;

    DefaultGeometrySerializer() {
      super(Geometry.class);
    }

    @Override
    public void serialize(
        Geometry value, JsonGenerator jsonGenerator, SerializerProvider serializers)
        throws IOException {
      jsonGenerator.writeString(value.asWellKnownText());
    }
  }

  /** The default Jackson module used by DSE Graph. */
  static class GraphSON1DefaultModule extends SimpleModule {

    private static final long serialVersionUID = 1L;

    GraphSON1DefaultModule(String name, Version version) {
      super(name, version, createDeserializers(), createSerializers());
    }

    private static Map<Class<?>, JsonDeserializer<?>> createDeserializers() {

      return ImmutableMap.<Class<?>, JsonDeserializer<?>>builder()

          // Inet (there is no built-in deserializer for InetAddress and subclasses)
          .put(InetAddress.class, new DefaultInetAddressDeserializer<>(InetAddress.class))
          .put(Inet4Address.class, new DefaultInetAddressDeserializer<>(Inet4Address.class))
          .put(Inet6Address.class, new DefaultInetAddressDeserializer<>(Inet6Address.class))

          // Geospatial types
          .put(Geometry.class, new DefaultGeometryDeserializer<>(Geometry.class))
          .put(Point.class, new DefaultGeometryDeserializer<>(Point.class))
          .put(LineString.class, new DefaultGeometryDeserializer<>(LineString.class))
          .put(Polygon.class, new DefaultGeometryDeserializer<>(Polygon.class))
          .build();
    }

    private static List<JsonSerializer<?>> createSerializers() {
      return ImmutableList.<JsonSerializer<?>>builder()
          .add(new DefaultGraphNodeSerializer())
          .add(new DefaultGeometrySerializer())
          .build();
    }
  }

  /** Serializers and deserializers for JSR 310 {@code java.time.*}. */
  static class GraphSON1JavaTimeModule extends SimpleModule {

    private static final long serialVersionUID = 1L;

    GraphSON1JavaTimeModule(String name, Version version) {
      super(name, version, createDeserializers(), createSerializers());
    }

    private static Map<Class<?>, JsonDeserializer<?>> createDeserializers() {

      return ImmutableMap.<Class<?>, JsonDeserializer<?>>builder()
          .put(java.time.Duration.class, new DurationJacksonDeserializer())
          .put(java.time.Instant.class, new InstantJacksonDeserializer())
          .put(java.time.LocalDate.class, new LocalDateJacksonDeserializer())
          .put(java.time.LocalTime.class, new LocalTimeJacksonDeserializer())
          .build();
    }

    private static List<JsonSerializer<?>> createSerializers() {
      return ImmutableList.<JsonSerializer<?>>builder()
          .add(new DurationJacksonSerializer())
          .add(new InstantJacksonSerializer())
          .add(new LocalDateJacksonSerializer())
          .add(new LocalTimeJacksonSerializer())
          .build();
    }
  }
}
