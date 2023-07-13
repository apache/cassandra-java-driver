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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.protocol.internal.request.RawBytesQuery;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

/**
 * Utility class to move boilerplate out of {@link GraphRequestHandler}.
 *
 * <p>We extend {@link Conversions} only for methods that can be directly reused as-is; if something
 * needs to be customized, it will be duplicated here instead of making the parent method
 * "pluggable".
 */
public class GraphConversions extends Conversions {

  static String GRAPH_LANG_OPTION_KEY = "graph-language";
  static String GRAPH_NAME_OPTION_KEY = "graph-name";
  static String GRAPH_SOURCE_OPTION_KEY = "graph-source";
  static String GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY = "graph-read-consistency";
  static String GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY = "graph-write-consistency";
  static String GRAPH_RESULTS_OPTION_KEY = "graph-results";
  static String GRAPH_TIMEOUT_OPTION_KEY = "request-timeout";

  static String inferSubProtocol(
      GraphStatement<?> statement, DriverExecutionProfile config, DefaultSession session) {
    String graphProtocol = statement.getSubProtocol();
    if (graphProtocol == null) {
      graphProtocol =
          config.getString(
              DseDriverOption.GRAPH_SUB_PROTOCOL,
              // TODO pick graphson-3.0 if the target graph uses the native engine
              "graphson-2.0");
    }
    assert graphProtocol != null;
    return graphProtocol;
  }

  static Message createMessageFromGraphStatement(
      GraphStatement<?> statement,
      String subProtocol,
      DriverExecutionProfile config,
      InternalDriverContext context) {

    ByteBuffer encodedQueryParams;
    try {
      Map<String, Object> queryParams =
          (statement instanceof ScriptGraphStatement)
              ? ((ScriptGraphStatement) statement).getQueryParams()
              : Collections.emptyMap();
      encodedQueryParams = GraphSONUtils.serializeToByteBuffer(queryParams, subProtocol);
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Couldn't serialize parameters for GraphStatement: " + statement, e);
    }

    int consistencyLevel =
        DefaultConsistencyLevel.valueOf(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
            .getProtocolCode();

    long timestamp = statement.getTimestamp();
    if (timestamp == Long.MIN_VALUE) {
      timestamp = context.getTimestampGenerator().next();
    }

    DseQueryOptions queryOptions =
        new DseQueryOptions(
            consistencyLevel,
            Collections.singletonList(encodedQueryParams),
            Collections.emptyMap(), // ignored by the DSE Graph server
            true, // also ignored
            50, // also ignored
            null, // also ignored
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // also ignored
            timestamp,
            null, // also ignored
            false, // also ignored
            null // also ignored
            );

    if (statement instanceof ScriptGraphStatement) {
      return new Query(((ScriptGraphStatement) statement).getScript(), queryOptions);
    } else {
      return new RawBytesQuery(getQueryBytes(statement, subProtocol), queryOptions);
    }
  }

  private static byte[] getQueryBytes(GraphStatement<?> statement, String graphSubProtocol) {
    assert statement instanceof FluentGraphStatement
        || statement instanceof BatchGraphStatement
        || statement instanceof BytecodeGraphStatement;
    Object toSerialize;
    if (statement instanceof FluentGraphStatement) {
      toSerialize = ((FluentGraphStatement) statement).getTraversal().asAdmin().getBytecode();
    } else if (statement instanceof BatchGraphStatement) {
      toSerialize = ((BatchGraphStatement) statement).iterator();
    } else {
      toSerialize = ((BytecodeGraphStatement) statement).getBytecode();
    }
    try {
      return GraphSONUtils.serializeToBytes(toSerialize, graphSubProtocol);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Map<String, ByteBuffer> createCustomPayload(
      GraphStatement<?> statement,
      String subProtocol,
      DriverExecutionProfile config,
      InternalDriverContext context) {

    ProtocolVersion protocolVersion = context.getProtocolVersion();

    NullAllowingImmutableMap.Builder<String, ByteBuffer> payload =
        NullAllowingImmutableMap.builder();
    Map<String, ByteBuffer> statementOptions = statement.getCustomPayload();
    payload.putAll(statementOptions);

    // Don't override anything that's already provided at the statement level
    if (!statementOptions.containsKey(GRAPH_LANG_OPTION_KEY)) {
      String graphLanguage =
          (statement instanceof ScriptGraphStatement) ? "gremlin-groovy" : "bytecode-json";
      payload.put(GRAPH_LANG_OPTION_KEY, TypeCodecs.TEXT.encode(graphLanguage, protocolVersion));
    }

    if (!isSystemQuery(statement, config)) {
      if (!statementOptions.containsKey(GRAPH_NAME_OPTION_KEY)) {
        String graphName = statement.getGraphName();
        if (graphName == null) {
          graphName = config.getString(DseDriverOption.GRAPH_NAME, null);
        }
        if (graphName != null) {
          payload.put(GRAPH_NAME_OPTION_KEY, TypeCodecs.TEXT.encode(graphName, protocolVersion));
        }
      }
      if (!statementOptions.containsKey(GRAPH_SOURCE_OPTION_KEY)) {
        String traversalSource = statement.getTraversalSource();
        if (traversalSource == null) {
          traversalSource = config.getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null);
        }
        if (traversalSource != null) {
          payload.put(
              GRAPH_SOURCE_OPTION_KEY, TypeCodecs.TEXT.encode(traversalSource, protocolVersion));
        }
      }
    }

    if (!statementOptions.containsKey(GRAPH_RESULTS_OPTION_KEY)) {
      assert subProtocol != null;
      payload.put(GRAPH_RESULTS_OPTION_KEY, TypeCodecs.TEXT.encode(subProtocol, protocolVersion));
    }

    if (!statementOptions.containsKey(GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY)) {
      ConsistencyLevel readCl = statement.getReadConsistencyLevel();
      String readClString =
          (readCl != null)
              ? readCl.name()
              : config.getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null);
      if (readClString != null) {
        payload.put(
            GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY,
            TypeCodecs.TEXT.encode(readClString, protocolVersion));
      }
    }

    if (!statementOptions.containsKey(GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY)) {
      ConsistencyLevel writeCl = statement.getWriteConsistencyLevel();
      String writeClString =
          (writeCl != null)
              ? writeCl.name()
              : config.getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null);
      if (writeClString != null) {
        payload.put(
            GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY,
            TypeCodecs.TEXT.encode(writeClString, protocolVersion));
      }
    }

    if (!statementOptions.containsKey(GRAPH_TIMEOUT_OPTION_KEY)) {
      Duration timeout = statement.getTimeout();
      if (timeout == null) {
        timeout = config.getDuration(DseDriverOption.GRAPH_TIMEOUT, null);
      }
      if (timeout != null && !timeout.isZero()) {
        payload.put(
            GRAPH_TIMEOUT_OPTION_KEY,
            TypeCodecs.BIGINT.encode(timeout.toMillis(), protocolVersion));
      }
    }
    return payload.build();
  }

  private static boolean isSystemQuery(GraphStatement<?> statement, DriverExecutionProfile config) {
    if (statement instanceof ScriptGraphStatement) {
      Boolean statementValue = ((ScriptGraphStatement) statement).isSystemQuery();
      if (statementValue != null) {
        return statementValue;
      }
    }
    return config.getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false);
  }
}
