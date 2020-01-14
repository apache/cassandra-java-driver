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

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.shaded.guava.common.base.Suppliers;
import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheLoader;
import com.datastax.oss.driver.shaded.guava.common.cache.LoadingCache;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONXModuleV2d0;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerIoRegistryV2d0;
import org.apache.tinkerpop.shaded.jackson.core.Version;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

public class GraphSONUtils {

  private static final LoadingCache<GraphProtocol, ObjectMapper> OBJECT_MAPPERS =
      CacheBuilder.newBuilder()
          .build(
              new CacheLoader<GraphProtocol, ObjectMapper>() {
                @Override
                public ObjectMapper load(@NonNull GraphProtocol graphSubProtocol) throws Exception {
                  switch (graphSubProtocol) {
                    case GRAPHSON_1_0:
                      com.datastax.oss.driver.api.core.Version driverVersion =
                          CqlSession.OSS_DRIVER_COORDINATES.getVersion();
                      Version driverJacksonVersion =
                          new Version(
                              driverVersion.getMajor(),
                              driverVersion.getMinor(),
                              driverVersion.getPatch(),
                              driverVersion.getPreReleaseLabels() != null
                                      && driverVersion.getPreReleaseLabels().contains("SNAPSHOT")
                                  ? "SNAPSHOT"
                                  : null,
                              "com.datastax.dse",
                              "dse-java-driver-core");

                      ObjectMapper mapper =
                          GraphSONMapper.build()
                              .version(GraphSONVersion.V1_0)
                              .create()
                              .createMapper();
                      mapper.registerModule(
                          new GraphSON1SerdeTP.GraphSON1DefaultModule(
                              "graph-graphson1default", driverJacksonVersion));
                      mapper.registerModule(
                          new GraphSON1SerdeTP.GraphSON1JavaTimeModule(
                              "graph-graphson1javatime", driverJacksonVersion));

                      return mapper;
                    case GRAPHSON_2_0:
                      return GraphSONMapper.build()
                          .version(GraphSONVersion.V2_0)
                          .addCustomModule(GraphSONXModuleV2d0.build().create(false))
                          .addRegistry(TinkerIoRegistryV2d0.instance())
                          .addCustomModule(new GraphSON2SerdeTP.DseGraphModule())
                          .addCustomModule(new GraphSON2SerdeTP.DriverObjectsModule())
                          .create()
                          .createMapper();

                    default:
                      throw new IllegalStateException(
                          String.format("GraphSON sub-protocol unknown: {%s}", graphSubProtocol));
                  }
                }
              });

  static final Supplier<GraphSONReader> GRAPHSON1_READER =
      Suppliers.memoize(
          () ->
              GraphSONReader.build()
                  .mapper(GraphSONMapper.build().version(GraphSONVersion.V1_0).create())
                  .create());

  public static ByteBuffer serializeToByteBuffer(Object object, GraphProtocol graphSubProtocol)
      throws IOException {
    return ByteBuffer.wrap(serializeToBytes(object, graphSubProtocol));
  }

  static byte[] serializeToBytes(Object object, GraphProtocol graphSubProtocol) throws IOException {
    try {
      return OBJECT_MAPPERS.get(graphSubProtocol).writeValueAsBytes(object);
    } catch (ExecutionException e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  public static GraphNode createGraphNode(List<ByteBuffer> data, GraphProtocol graphSubProtocol)
      throws IOException {
    try {
      ObjectMapper mapper = OBJECT_MAPPERS.get(graphSubProtocol);
      switch (graphSubProtocol) {
        case GRAPHSON_1_0:
          return new LegacyGraphNode(mapper.readTree(Bytes.getArray(data.get(0))), mapper);
        case GRAPHSON_2_0:
          return new ObjectGraphNode(mapper.readValue(Bytes.getArray(data.get(0)), Object.class));
        default:
          // Should already be caught when we lookup in the cache
          throw new AssertionError(
              String.format("Unknown GraphSON sub-protocol: {%s}", graphSubProtocol));
      }
    } catch (ExecutionException e) {
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }
}
