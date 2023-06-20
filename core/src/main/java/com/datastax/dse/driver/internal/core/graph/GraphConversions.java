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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.dse.driver.internal.core.graph.binary.buffer.DseNettyBufferFactory;
import com.datastax.dse.protocol.internal.request.RawBytesQuery;
import com.datastax.dse.protocol.internal.request.query.ContinuousPagingOptions;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;

/**
 * Utility class to move boilerplate out of {@link GraphRequestHandler}.
 *
 * <p>We extend {@link Conversions} only for methods that can be directly reused as-is; if something
 * needs to be customized, it will be duplicated here instead of making the parent method
 * "pluggable".
 */
public class GraphConversions extends Conversions {

  static final String GRAPH_LANG_OPTION_KEY = "graph-language";
  static final String GRAPH_NAME_OPTION_KEY = "graph-name";
  static final String GRAPH_SOURCE_OPTION_KEY = "graph-source";
  static final String GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY = "graph-read-consistency";
  static final String GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY = "graph-write-consistency";
  static final String GRAPH_RESULTS_OPTION_KEY = "graph-results";
  static final String GRAPH_TIMEOUT_OPTION_KEY = "request-timeout";
  static final String GRAPH_BINARY_QUERY_OPTION_KEY = "graph-binary-query";

  static final String LANGUAGE_GROOVY = "gremlin-groovy";
  static final String LANGUAGE_BYTECODE = "bytecode-json";

  private static final BufferFactory<ByteBuf> FACTORY = new DseNettyBufferFactory();

  @VisibleForTesting static final byte[] EMPTY_STRING_QUERY = "".getBytes(UTF_8);

  public static Message createContinuousMessageFromGraphStatement(
      GraphStatement<?> statement,
      GraphProtocol subProtocol,
      DriverExecutionProfile config,
      InternalDriverContext context,
      GraphBinaryModule graphBinaryModule) {

    final List<ByteBuffer> encodedQueryParams;
    if (!(statement instanceof ScriptGraphStatement)
        || ((ScriptGraphStatement) statement).getQueryParams().isEmpty()) {
      encodedQueryParams = Collections.emptyList();
    } else {
      try {
        Map<String, Object> queryParams = ((ScriptGraphStatement) statement).getQueryParams();
        if (subProtocol.isGraphBinary()) {
          Buffer graphBinaryParams = graphBinaryModule.serialize(queryParams);
          encodedQueryParams = Collections.singletonList(graphBinaryParams.nioBuffer());
          graphBinaryParams.release();
        } else {
          encodedQueryParams =
              Collections.singletonList(
                  GraphSONUtils.serializeToByteBuffer(queryParams, subProtocol));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Couldn't serialize parameters for GraphStatement: " + statement, e);
      }
    }

    int consistencyLevel =
        DefaultConsistencyLevel.valueOf(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
            .getProtocolCode();

    long timestamp = statement.getTimestamp();
    if (timestamp == Statement.NO_DEFAULT_TIMESTAMP) {
      timestamp = context.getTimestampGenerator().next();
    }

    int pageSize = config.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_PAGE_SIZE);
    int maxPages = config.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES);
    int maxPagesPerSecond =
        config.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND);
    int maxEnqueuedPages =
        config.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES);
    ContinuousPagingOptions options =
        new ContinuousPagingOptions(maxPages, maxPagesPerSecond, maxEnqueuedPages);

    DseQueryOptions queryOptions =
        new DseQueryOptions(
            consistencyLevel,
            encodedQueryParams,
            Collections.emptyMap(), // ignored by the DSE Graph server
            true, // also ignored
            pageSize,
            null,
            ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL, // also ignored
            timestamp,
            null, // also ignored
            false, // graph CP does not support sizeInBytes
            options);

    if (statement instanceof ScriptGraphStatement) {
      return new Query(((ScriptGraphStatement) statement).getScript(), queryOptions);
    } else {
      return new RawBytesQuery(getQueryBytes(statement, subProtocol), queryOptions);
    }
  }

  static Message createMessageFromGraphStatement(
      GraphStatement<?> statement,
      GraphProtocol subProtocol,
      DriverExecutionProfile config,
      InternalDriverContext context,
      GraphBinaryModule graphBinaryModule) {

    final List<ByteBuffer> encodedQueryParams;
    if (!(statement instanceof ScriptGraphStatement)
        || ((ScriptGraphStatement) statement).getQueryParams().isEmpty()) {
      encodedQueryParams = Collections.emptyList();
    } else {
      try {
        Map<String, Object> queryParams = ((ScriptGraphStatement) statement).getQueryParams();
        if (subProtocol.isGraphBinary()) {
          Buffer graphBinaryParams = graphBinaryModule.serialize(queryParams);
          encodedQueryParams = Collections.singletonList(graphBinaryParams.nioBuffer());
          graphBinaryParams.release();
        } else {
          encodedQueryParams =
              Collections.singletonList(
                  GraphSONUtils.serializeToByteBuffer(queryParams, subProtocol));
        }
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Couldn't serialize parameters for GraphStatement: " + statement, e);
      }
    }

    ConsistencyLevel consistency = statement.getConsistencyLevel();
    int consistencyLevel =
        (consistency == null)
            ? context
                .getConsistencyLevelRegistry()
                .nameToCode(config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
            : consistency.getProtocolCode();

    long timestamp = statement.getTimestamp();
    if (timestamp == Statement.NO_DEFAULT_TIMESTAMP) {
      timestamp = context.getTimestampGenerator().next();
    }

    DseQueryOptions queryOptions =
        new DseQueryOptions(
            consistencyLevel,
            encodedQueryParams,
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

  // This method returns either a Bytecode object, or a List<Bytecode> if the statement is a
  // BatchGraphStatement
  @VisibleForTesting
  public static Object bytecodeToSerialize(GraphStatement<?> statement) {
    Preconditions.checkArgument(
        statement instanceof FluentGraphStatement
            || statement instanceof BatchGraphStatement
            || statement instanceof BytecodeGraphStatement,
        "To serialize bytecode the query must be a fluent or batch statement, but was: %s",
        statement.getClass());

    Object toSerialize;
    if (statement instanceof FluentGraphStatement) {
      toSerialize = ((FluentGraphStatement) statement).getTraversal().asAdmin().getBytecode();
    } else if (statement instanceof BatchGraphStatement) {
      // transform the Iterator<GraphTraversal> to List<Bytecode>
      toSerialize =
          ImmutableList.copyOf(
              Iterators.transform(
                  ((BatchGraphStatement) statement).iterator(),
                  traversal -> traversal.asAdmin().getBytecode()));
    } else {
      toSerialize = ((BytecodeGraphStatement) statement).getBytecode();
    }
    return toSerialize;
  }

  private static byte[] getQueryBytes(GraphStatement<?> statement, GraphProtocol graphSubProtocol) {
    try {
      return graphSubProtocol.isGraphBinary()
          // if GraphBinary, the query is encoded in the custom payload, and not in the query field
          // see GraphConversions#createCustomPayload()
          ? EMPTY_STRING_QUERY
          : GraphSONUtils.serializeToBytes(bytecodeToSerialize(statement), graphSubProtocol);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static Map<String, ByteBuffer> createCustomPayload(
      GraphStatement<?> statement,
      GraphProtocol subProtocol,
      DriverExecutionProfile config,
      InternalDriverContext context,
      GraphBinaryModule graphBinaryModule) {

    ProtocolVersion protocolVersion = context.getProtocolVersion();

    NullAllowingImmutableMap.Builder<String, ByteBuffer> payload =
        NullAllowingImmutableMap.builder();
    Map<String, ByteBuffer> statementOptions = statement.getCustomPayload();
    payload.putAll(statementOptions);

    final String graphLanguage;

    // Don't override anything that's already provided at the statement level
    if (!statementOptions.containsKey(GRAPH_LANG_OPTION_KEY)) {
      graphLanguage =
          statement instanceof ScriptGraphStatement ? LANGUAGE_GROOVY : LANGUAGE_BYTECODE;
      payload.put(GRAPH_LANG_OPTION_KEY, TypeCodecs.TEXT.encode(graphLanguage, protocolVersion));
    } else {
      graphLanguage =
          TypeCodecs.TEXT.decode(statementOptions.get(GRAPH_LANG_OPTION_KEY), protocolVersion);
      Preconditions.checkNotNull(
          graphLanguage, "A null value was set for the graph-language custom payload key.");
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

    // the payload allows null entry values so doing a get directly here and checking for null
    final ByteBuffer payloadInitialProtocol = statementOptions.get(GRAPH_RESULTS_OPTION_KEY);
    if (payloadInitialProtocol == null) {
      Preconditions.checkNotNull(subProtocol);
      payload.put(
          GRAPH_RESULTS_OPTION_KEY,
          TypeCodecs.TEXT.encode(subProtocol.toInternalCode(), protocolVersion));
    } else {
      subProtocol =
          GraphProtocol.fromString(TypeCodecs.TEXT.decode(payloadInitialProtocol, protocolVersion));
    }

    if (subProtocol.isGraphBinary() && graphLanguage.equals(LANGUAGE_BYTECODE)) {
      Object bytecodeQuery = bytecodeToSerialize(statement);
      try {
        Buffer bytecodeByteBuf = graphBinaryModule.serialize(bytecodeQuery);
        payload.put(GRAPH_BINARY_QUERY_OPTION_KEY, bytecodeByteBuf.nioBuffer());
        bytecodeByteBuf.release();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    if (!statementOptions.containsKey(GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY)) {
      ConsistencyLevel readCl = statement.getReadConsistencyLevel();
      String readClString =
          readCl != null
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
          writeCl != null
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
        timeout = config.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO);
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

  public static GraphNode createGraphBinaryGraphNode(
      List<ByteBuffer> data, GraphBinaryModule graphBinaryModule) throws IOException {
    // there should be only one column in the given row
    Preconditions.checkArgument(data.size() == 1, "Invalid row given to deserialize");

    Buffer toDeserialize = FACTORY.wrap(data.get(0));
    Object deserializedObject = graphBinaryModule.deserialize(toDeserialize);
    toDeserialize.release();
    assert deserializedObject instanceof Traverser
        : "Graph protocol error. Received object should be a Traverser but it is not.";
    return new ObjectGraphNode(deserializedObject);
  }

  public static Duration resolveGraphRequestTimeout(
      GraphStatement<?> statement, InternalDriverContext context) {
    DriverExecutionProfile executionProfile = resolveExecutionProfile(statement, context);
    return statement.getTimeout() != null
        ? statement.getTimeout()
        : executionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT);
  }

  public static GraphProtocol resolveGraphSubProtocol(
      GraphStatement<?> statement,
      GraphSupportChecker graphSupportChecker,
      InternalDriverContext context) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    return graphSupportChecker.inferGraphProtocol(statement, executionProfile, context);
  }
}
