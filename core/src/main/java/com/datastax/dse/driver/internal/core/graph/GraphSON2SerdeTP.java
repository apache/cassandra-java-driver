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
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.dse.driver.api.core.graph.predicates.Search;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.structure.io.graphson.AbstractObjectDeserializer;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.SerializerProvider;
import org.apache.tinkerpop.shaded.jackson.databind.deser.std.StdDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdScalarSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.ser.std.StdSerializer;

public class GraphSON2SerdeTP {

  /**
   * A Jackson Module to use for TinkerPop serialization/deserialization. It extends {@link
   * org.apache.tinkerpop.gremlin.structure.io.graphson.TinkerPopJacksonModule} because of the
   * specific typing format used in GraphSON.
   */
  public static class DseGraphModule extends TinkerPopJacksonModule {

    private static final long serialVersionUID = 1L;

    public DseGraphModule() {
      super("dse-driver-2.0");
      addSerializer(DefaultPoint.class, new PointGeometrySerializer());
      addSerializer(DefaultLineString.class, new LineStringGeometrySerializer());
      addSerializer(DefaultPolygon.class, new PolygonGeometrySerializer());
      addSerializer(Distance.class, new DistanceGeometrySerializer());
      // override TinkerPop's P predicates because of DSE's Search and Geo predicates
      addSerializer(P.class, new DsePJacksonSerializer());
      addSerializer(EditDistance.class, new EditDistanceSerializer());

      addDeserializer(DefaultLineString.class, new LineStringGeometryDeserializer());
      addDeserializer(DefaultPoint.class, new PointGeometryDeserializer());
      addDeserializer(DefaultPolygon.class, new PolygonGeometryDeserializer());
      addDeserializer(Distance.class, new DistanceGeometryDeserializer());
      // override TinkerPop's P predicates because of DSE's Search and Geo predicates
      addDeserializer(P.class, new DsePJacksonDeserializer());
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<Class, String> getTypeDefinitions() {
      Map<Class, String> definitions = new HashMap<>();
      definitions.put(DefaultLineString.class, "LineString");
      definitions.put(DefaultPoint.class, "Point");
      definitions.put(DefaultPolygon.class, "Polygon");
      definitions.put(byte[].class, "Blob");
      definitions.put(Distance.class, "Distance");
      definitions.put(P.class, "P");
      return definitions;
    }

    @Override
    public String getTypeNamespace() {
      return "dse";
    }

    abstract static class AbstractGeometryJacksonDeserializer<T extends Geometry>
        extends StdDeserializer<T> {

      private static final long serialVersionUID = 1L;

      AbstractGeometryJacksonDeserializer(final Class<T> clazz) {
        super(clazz);
      }

      public abstract T parse(final String val);

      @Override
      public T deserialize(
          final JsonParser jsonParser, final DeserializationContext deserializationContext)
          throws IOException {
        return parse(jsonParser.getText());
      }
    }

    abstract static class AbstractGeometryJacksonSerializer<T extends Geometry>
        extends StdScalarSerializer<T> {

      private static final long serialVersionUID = 1L;

      AbstractGeometryJacksonSerializer(final Class<T> clazz) {
        super(clazz);
      }

      @Override
      public void serialize(
          final T value, final JsonGenerator gen, final SerializerProvider serializerProvider)
          throws IOException {
        gen.writeString(value.asWellKnownText());
      }
    }

    public static class LineStringGeometrySerializer
        extends AbstractGeometryJacksonSerializer<LineString> {

      private static final long serialVersionUID = 1L;

      LineStringGeometrySerializer() {
        super(LineString.class);
      }
    }

    public static class LineStringGeometryDeserializer
        extends AbstractGeometryJacksonDeserializer<DefaultLineString> {

      private static final long serialVersionUID = 1L;

      LineStringGeometryDeserializer() {
        super(DefaultLineString.class);
      }

      @Override
      public DefaultLineString parse(final String val) {
        return (DefaultLineString) LineString.fromWellKnownText(val);
      }
    }

    public static class PolygonGeometrySerializer
        extends AbstractGeometryJacksonSerializer<Polygon> {

      private static final long serialVersionUID = 1L;

      PolygonGeometrySerializer() {
        super(Polygon.class);
      }
    }

    public static class PolygonGeometryDeserializer
        extends AbstractGeometryJacksonDeserializer<DefaultPolygon> {

      private static final long serialVersionUID = 1L;

      PolygonGeometryDeserializer() {
        super(DefaultPolygon.class);
      }

      @Override
      public DefaultPolygon parse(final String val) {
        return (DefaultPolygon) Polygon.fromWellKnownText(val);
      }
    }

    public static class PointGeometrySerializer extends AbstractGeometryJacksonSerializer<Point> {

      private static final long serialVersionUID = 1L;

      PointGeometrySerializer() {
        super(Point.class);
      }
    }

    public static class PointGeometryDeserializer
        extends AbstractGeometryJacksonDeserializer<DefaultPoint> {

      private static final long serialVersionUID = 1L;

      PointGeometryDeserializer() {
        super(DefaultPoint.class);
      }

      @Override
      public DefaultPoint parse(final String val) {
        return (DefaultPoint) Point.fromWellKnownText(val);
      }
    }

    public static class DistanceGeometrySerializer
        extends AbstractGeometryJacksonSerializer<Distance> {

      private static final long serialVersionUID = 1L;

      DistanceGeometrySerializer() {
        super(Distance.class);
      }
    }

    public static class DistanceGeometryDeserializer
        extends AbstractGeometryJacksonDeserializer<Distance> {

      private static final long serialVersionUID = 1L;

      DistanceGeometryDeserializer() {
        super(Distance.class);
      }

      @Override
      public Distance parse(final String val) {
        return Distance.fromWellKnownText(val);
      }
    }

    @SuppressWarnings("rawtypes")
    static final class DsePJacksonSerializer extends StdScalarSerializer<P> {

      private static final long serialVersionUID = 1L;

      DsePJacksonSerializer() {
        super(P.class);
      }

      @Override
      public void serialize(
          final P p, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
          throws IOException {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeStringField("predicateType", getPredicateType(p));
        jsonGenerator.writeStringField(
            GraphSONTokens.PREDICATE,
            p instanceof ConnectiveP
                ? p instanceof AndP ? GraphSONTokens.AND : GraphSONTokens.OR
                : p.getBiPredicate().toString());
        if (p instanceof ConnectiveP) {
          jsonGenerator.writeArrayFieldStart(GraphSONTokens.VALUE);
          for (final P<?> predicate : ((ConnectiveP<?>) p).getPredicates()) {
            jsonGenerator.writeObject(predicate);
          }
          jsonGenerator.writeEndArray();
        } else {
          if (p.getValue() instanceof Collection) {
            jsonGenerator.writeArrayFieldStart(GraphSONTokens.VALUE);
            for (final Object object : (Collection) p.getValue()) {
              jsonGenerator.writeObject(object);
            }
            jsonGenerator.writeEndArray();
          } else {
            jsonGenerator.writeObjectField(GraphSONTokens.VALUE, p.getValue());
          }
        }
        jsonGenerator.writeEndObject();
      }

      private String getPredicateType(P p) {
        if (p.getBiPredicate() instanceof SearchPredicate) {
          return Search.class.getSimpleName();
        } else if (p.getBiPredicate() instanceof GeoPredicate) {
          return Geo.class.getSimpleName();
        } else {
          return P.class.getSimpleName();
        }
      }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    static final class DsePJacksonDeserializer extends AbstractObjectDeserializer<P> {

      private static final long serialVersionUID = 1L;

      DsePJacksonDeserializer() {
        super(P.class);
      }

      @Override
      public P createObject(final Map<String, Object> data) {
        final String predicate = (String) data.get(GraphSONTokens.PREDICATE);
        final String predicateType = (String) data.get("predicateType");
        final Object value = data.get(GraphSONTokens.VALUE);
        if (predicate.equals(GraphSONTokens.AND) || predicate.equals(GraphSONTokens.OR)) {
          return predicate.equals(GraphSONTokens.AND)
              ? new AndP((List<P>) value)
              : new OrP((List<P>) value);
        } else {
          try {
            if (value instanceof Collection) {
              if (predicate.equals("between")) {
                return P.between(((List) value).get(0), ((List) value).get(1));
              } else if (predicateType.equals(P.class.getSimpleName())
                  && predicate.equals("inside")) {
                return P.between(((List) value).get(0), ((List) value).get(1));
              } else if (predicate.equals("outside")) {
                return P.outside(((List) value).get(0), ((List) value).get(1));
              } else if (predicate.equals("within")) {
                return P.within((Collection) value);
              } else if (predicate.equals("without")) {
                return P.without((Collection) value);
              } else {
                return (P)
                    P.class.getMethod(predicate, Collection.class).invoke(null, (Collection) value);
              }
            } else {
              if (predicate.equals(SearchPredicate.prefix.name())) {
                return Search.prefix((String) value);
              } else if (predicate.equals(SearchPredicate.tokenPrefix.name())) {
                return Search.tokenPrefix((String) value);
              } else if (predicate.equals(SearchPredicate.regex.name())) {
                return Search.regex((String) value);
              } else if (predicate.equals(SearchPredicate.tokenRegex.name())) {
                return Search.tokenRegex((String) value);
              } else if (predicate.equals(SearchPredicate.token.name())) {
                return Search.token((String) value);
              } else if (predicate.equals(SearchPredicate.fuzzy.name())) {
                Map<String, Object> arguments = (Map<String, Object>) value;
                return Search.fuzzy(
                    (String) arguments.get("query"), (int) arguments.get("distance"));
              } else if (predicate.equals(SearchPredicate.tokenFuzzy.name())) {
                Map<String, Object> arguments = (Map<String, Object>) value;
                return Search.tokenFuzzy(
                    (String) arguments.get("query"), (int) arguments.get("distance"));
              } else if (predicate.equals(SearchPredicate.phrase.name())) {
                Map<String, Object> arguments = (Map<String, Object>) value;
                return Search.phrase(
                    (String) arguments.get("query"), (int) arguments.get("distance"));
              } else if (predicateType.equals(Geo.class.getSimpleName())
                  && predicate.equals(GeoPredicate.inside.name())) {
                return Geo.inside(
                    ((Distance) value).getCenter(),
                    ((Distance) value).getRadius(),
                    Geo.Unit.DEGREES);
              } else if (predicateType.equals(Geo.class.getSimpleName())
                  && predicate.equals(GeoPredicate.insideCartesian.name())) {
                return Geo.inside(((Distance) value).getCenter(), ((Distance) value).getRadius());
              } else {
                return (P) P.class.getMethod(predicate, Object.class).invoke(null, value);
              }
            }
          } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
          }
        }
      }
    }

    public static class EditDistanceSerializer extends StdSerializer<EditDistance> {

      private static final long serialVersionUID = 1L;

      EditDistanceSerializer() {
        super(EditDistance.class);
      }

      @Override
      public void serialize(
          EditDistance editDistance, JsonGenerator generator, SerializerProvider provider)
          throws IOException {
        generator.writeObject(
            ImmutableMap.of("query", editDistance.query, "distance", editDistance.distance));
      }

      @Override
      public void serializeWithType(
          EditDistance editDistance,
          JsonGenerator generator,
          SerializerProvider provider,
          TypeSerializer serializer)
          throws IOException {
        serialize(editDistance, generator, provider);
      }
    }
  }

  public static class DriverObjectsModule extends SimpleModule {

    private static final long serialVersionUID = 1L;

    public DriverObjectsModule() {
      super("datastax-driver-module");
      addSerializer(ObjectGraphNode.class, new ObjectGraphNodeGraphSON2Serializer());
    }

    static final class ObjectGraphNodeGraphSON2Serializer extends StdSerializer<ObjectGraphNode> {

      private static final long serialVersionUID = 1L;

      protected ObjectGraphNodeGraphSON2Serializer() {
        super(ObjectGraphNode.class);
      }

      @Override
      public void serialize(
          ObjectGraphNode objectGraphNode,
          JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider)
          throws IOException {
        jsonGenerator.writeObject(objectGraphNode.as(Object.class));
      }

      @Override
      public void serializeWithType(
          ObjectGraphNode objectGraphNode,
          JsonGenerator jsonGenerator,
          SerializerProvider serializerProvider,
          TypeSerializer typeSerializer)
          throws IOException {
        serialize(objectGraphNode, jsonGenerator, serializerProvider);
      }
    }
  }
}
