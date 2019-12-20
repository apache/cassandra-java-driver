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
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.dse.driver.internal.core.graph.EditDistance;
import com.datastax.dse.driver.internal.core.graph.binary.buffer.DseNettyBufferFactory;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.javatuples.Pair;

public class GraphBinaryModule {
  public static final UnpooledByteBufAllocator ALLOCATOR = new UnpooledByteBufAllocator(false);
  private static final BufferFactory<ByteBuf> FACTORY = new DseNettyBufferFactory();

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
      DefaultDriverContext driverContext) {
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
  public <T> T deserialize(final Buffer buffer) throws IOException {
    return reader.read(buffer);
  }

  public <T> Buffer serialize(final T value) throws IOException {
    return serialize(value, FACTORY.create(ALLOCATOR.heapBuffer()));
  }

  public <T> Buffer serialize(final T value, final Buffer buffer) throws IOException {
    try {
      writer.write(value, buffer);
      return buffer;
    } catch (Exception e) {
      buffer.release();
      throw e;
    }
  }
}
