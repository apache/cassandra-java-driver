/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.dse.driver.internal.core.graph.EditDistance;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.TypeSerializerRegistry;
import org.javatuples.Pair;

public class GraphBinaryModule {
  public static final UnpooledByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);

  static final String GRAPH_BINARY_POINT_TYPE_NAME = "driver.dse.geometry.Point";
  static final String GRAPH_BINARY_LINESTRING_TYPE_NAME = "driver.dse.geometry.LineString";
  static final String GRAPH_BINARY_POLYGON_TYPE_NAME = "driver.dse.geometry.Polygon";
  static final String GRAPH_BINARY_DISTANCE_TYPE_NAME = "driver.dse.geometry.Distance";
  static final String GRAPH_BINARY_DURATION_TYPE_NAME = "driver.core.Duration";
  static final String GRAPH_BINARY_EDIT_DISTANCE_TYPE_NAME = "driver.dse.search.EditDistance";
  static final String GRAPH_BINARY_TUPLE_VALUE_TYPE_NAME = "driver.core.TupleValue";
  static final String GRAPH_BINARY_UDT_VALUE_TYPE_NAME = "driver.core.UDTValue";
  static final String GRAPH_BINARY_PAIR_TYPE_NAME = "org.javatuples.Pair";

  private final GraphBinaryReader reader;
  private final GraphBinaryWriter writer;

  public GraphBinaryModule(GraphBinaryReader reader, GraphBinaryWriter writer) {
    this.reader = reader;
    this.writer = writer;
  }

  public static TypeSerializerRegistry createDseTypeSerializerRegistry(
      DseDriverContext driverContext) {
    return TypeSerializerRegistry.build()
        .addCustomType(CqlDuration.class, new CqlDurationSerializer())
        .addCustomType(Point.class, new PointSerializer())
        .addCustomType(LineString.class, new LineStringSerializer())
        .addCustomType(Polygon.class, new PolygonSerializer())
        .addCustomType(Distance.class, new DistanceSerializer())
        .addCustomType(EditDistance.class, new EditDistanceSerializer())
        .addCustomType(TupleValue.class, new TupleValueSerializer(driverContext))
        .addCustomType(UdtValue.class, new UdtValueSerializer(driverContext))
        .addCustomType(Pair.class, new PairSerializer())
        .create();
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> T deserialize(final ByteBuf buffer) throws SerializationException {
    return reader.read(buffer);
  }

  public <T> ByteBuf serialize(final T value) throws SerializationException {
    return serialize(value, ALLOCATOR);
  }

  public <T> ByteBuf serialize(final T value, final ByteBufAllocator allocator)
      throws SerializationException {
    return serialize(value, allocator.heapBuffer());
  }

  public <T> ByteBuf serialize(final T value, final ByteBuf buffer) throws SerializationException {
    try {
      writer.write(value, buffer);
      return buffer;
    } catch (Exception e) {
      buffer.release();
      throw e;
    }
  }
}
