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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CASWriteUnknownException;
import com.datastax.oss.driver.api.core.servererrors.CDCWriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.InvalidConfigurationInQueryException;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.UnauthorizedException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.driver.internal.core.metadata.PartitionerFactory;
import com.datastax.oss.driver.internal.core.protocol.LwtInfo;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.primitives.Ints;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.CASWriteUnknown;
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Utility methods to convert to/from protocol messages.
 *
 * <p>The main goal of this class is to move this code out of the request handlers.
 */
public class Conversions {

  public static DriverExecutionProfile resolveExecutionProfile(
      Request request, DriverContext context) {
    if (request.getExecutionProfile() != null) {
      return request.getExecutionProfile();
    } else {
      DriverConfig config = context.getConfig();
      String profileName = request.getExecutionProfileName();
      return (profileName == null || profileName.isEmpty())
          ? config.getDefaultProfile()
          : config.getProfile(profileName);
    }
  }

  public static Message toMessage(
      Statement<?> statement, DriverExecutionProfile config, InternalDriverContext context) {
    ConsistencyLevelRegistry consistencyLevelRegistry = context.getConsistencyLevelRegistry();
    ConsistencyLevel consistency = statement.getConsistencyLevel();
    int consistencyCode =
        (consistency == null)
            ? consistencyLevelRegistry.nameToCode(
                config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
            : consistency.getProtocolCode();
    int pageSize = statement.getPageSize();
    if (pageSize <= 0) {
      pageSize = config.getInt(DefaultDriverOption.REQUEST_PAGE_SIZE);
    }
    ConsistencyLevel serialConsistency = statement.getSerialConsistencyLevel();
    int serialConsistencyCode =
        (serialConsistency == null)
            ? consistencyLevelRegistry.nameToCode(
                config.getString(DefaultDriverOption.REQUEST_SERIAL_CONSISTENCY))
            : serialConsistency.getProtocolCode();
    long timestamp = statement.getQueryTimestamp();
    if (timestamp == Statement.NO_DEFAULT_TIMESTAMP) {
      timestamp = context.getTimestampGenerator().next();
    }
    CodecRegistry codecRegistry = context.getCodecRegistry();
    ProtocolVersion protocolVersion = context.getProtocolVersion();
    ProtocolVersionRegistry protocolVersionRegistry = context.getProtocolVersionRegistry();
    CqlIdentifier keyspace = statement.getKeyspace();
    int nowInSeconds = statement.getNowInSeconds();
    if (nowInSeconds != Statement.NO_NOW_IN_SECONDS
        && !protocolVersionRegistry.supports(
            protocolVersion, DefaultProtocolFeature.NOW_IN_SECONDS)) {
      throw new IllegalArgumentException("Can't use nowInSeconds with protocol " + protocolVersion);
    }
    if (statement instanceof SimpleStatement) {
      SimpleStatement simpleStatement = (SimpleStatement) statement;
      List<Object> positionalValues = simpleStatement.getPositionalValues();
      Map<CqlIdentifier, Object> namedValues = simpleStatement.getNamedValues();
      if (!positionalValues.isEmpty() && !namedValues.isEmpty()) {
        throw new IllegalArgumentException(
            "Can't have both positional and named values in a statement.");
      }
      if (keyspace != null
          && !protocolVersionRegistry.supports(
              protocolVersion, DefaultProtocolFeature.PER_REQUEST_KEYSPACE)) {
        throw new IllegalArgumentException(
            "Can't use per-request keyspace with protocol " + protocolVersion);
      }
      QueryOptions queryOptions =
          new QueryOptions(
              consistencyCode,
              encode(positionalValues, codecRegistry, protocolVersion),
              encode(namedValues, codecRegistry, protocolVersion),
              false,
              pageSize,
              statement.getPagingState(),
              serialConsistencyCode,
              timestamp,
              (keyspace == null) ? null : keyspace.asInternal(),
              nowInSeconds);
      return new Query(simpleStatement.getQuery(), queryOptions);
    } else if (statement instanceof BoundStatement) {
      BoundStatement boundStatement = (BoundStatement) statement;
      if (!protocolVersionRegistry.supports(
          protocolVersion, DefaultProtocolFeature.UNSET_BOUND_VALUES)) {
        ensureAllSet(boundStatement);
      }
      boolean skipMetadata =
          boundStatement.getPreparedStatement().getResultSetDefinitions().size() > 0;
      QueryOptions queryOptions =
          new QueryOptions(
              consistencyCode,
              boundStatement.getValues(),
              Collections.emptyMap(),
              skipMetadata,
              pageSize,
              statement.getPagingState(),
              serialConsistencyCode,
              timestamp,
              null,
              nowInSeconds);
      PreparedStatement preparedStatement = boundStatement.getPreparedStatement();
      ByteBuffer id = preparedStatement.getId();
      ByteBuffer resultMetadataId = preparedStatement.getResultMetadataId();
      return new Execute(
          Bytes.getArray(id),
          (resultMetadataId == null) ? null : Bytes.getArray(resultMetadataId),
          queryOptions);
    } else if (statement instanceof BatchStatement) {
      BatchStatement batchStatement = (BatchStatement) statement;
      if (!protocolVersionRegistry.supports(
          protocolVersion, DefaultProtocolFeature.UNSET_BOUND_VALUES)) {
        ensureAllSet(batchStatement);
      }
      if (keyspace != null
          && !protocolVersionRegistry.supports(
              protocolVersion, DefaultProtocolFeature.PER_REQUEST_KEYSPACE)) {
        throw new IllegalArgumentException(
            "Can't use per-request keyspace with protocol " + protocolVersion);
      }
      List<Object> queriesOrIds = new ArrayList<>(batchStatement.size());
      List<List<ByteBuffer>> values = new ArrayList<>(batchStatement.size());
      for (BatchableStatement<?> child : batchStatement) {
        if (child instanceof SimpleStatement) {
          SimpleStatement simpleStatement = (SimpleStatement) child;
          if (simpleStatement.getNamedValues().size() > 0) {
            throw new IllegalArgumentException(
                String.format(
                    "Batch statements cannot contain simple statements with named values "
                        + "(offending statement: %s)",
                    simpleStatement.getQuery()));
          }
          queriesOrIds.add(simpleStatement.getQuery());
          values.add(encode(simpleStatement.getPositionalValues(), codecRegistry, protocolVersion));
        } else if (child instanceof BoundStatement) {
          BoundStatement boundStatement = (BoundStatement) child;
          queriesOrIds.add(Bytes.getArray(boundStatement.getPreparedStatement().getId()));
          values.add(boundStatement.getValues());
        } else {
          throw new IllegalArgumentException(
              "Unsupported child statement: " + child.getClass().getName());
        }
      }
      return new Batch(
          batchStatement.getBatchType().getProtocolCode(),
          queriesOrIds,
          values,
          consistencyCode,
          serialConsistencyCode,
          timestamp,
          (keyspace == null) ? null : keyspace.asInternal(),
          nowInSeconds);
    } else {
      throw new IllegalArgumentException(
          "Unsupported statement type: " + statement.getClass().getName());
    }
  }

  public static List<ByteBuffer> encode(
      List<Object> values, CodecRegistry codecRegistry, ProtocolVersion protocolVersion) {
    if (values.isEmpty()) {
      return Collections.emptyList();
    } else {
      ByteBuffer[] encodedValues = new ByteBuffer[values.size()];
      int i = 0;
      for (Object value : values) {
        encodedValues[i++] =
            (value == null)
                ? null
                : ValuesHelper.encodeToDefaultCqlMapping(value, codecRegistry, protocolVersion);
      }
      return NullAllowingImmutableList.of(encodedValues);
    }
  }

  public static Map<String, ByteBuffer> encode(
      Map<CqlIdentifier, Object> values,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    if (values.isEmpty()) {
      return Collections.emptyMap();
    } else {
      NullAllowingImmutableMap.Builder<String, ByteBuffer> encodedValues =
          NullAllowingImmutableMap.builder(values.size());
      for (Map.Entry<CqlIdentifier, Object> entry : values.entrySet()) {
        if (entry.getValue() == null) {
          encodedValues.put(entry.getKey().asInternal(), null);
        } else {
          encodedValues.put(
              entry.getKey().asInternal(),
              ValuesHelper.encodeToDefaultCqlMapping(
                  entry.getValue(), codecRegistry, protocolVersion));
        }
      }
      return encodedValues.build();
    }
  }

  public static void ensureAllSet(BoundStatement boundStatement) {
    for (int i = 0; i < boundStatement.size(); i++) {
      if (!boundStatement.isSet(i)) {
        throw new IllegalStateException(
            "Unset value at index "
                + i
                + ". "
                + "If you want this value to be null, please set it to null explicitly.");
      }
    }
  }

  public static void ensureAllSet(BatchStatement batchStatement) {
    for (BatchableStatement<?> batchableStatement : batchStatement) {
      if (batchableStatement instanceof BoundStatement) {
        ensureAllSet(((BoundStatement) batchableStatement));
      }
    }
  }

  public static AsyncResultSet toResultSet(
      Result result,
      ExecutionInfo executionInfo,
      CqlSession session,
      InternalDriverContext context) {
    if (result instanceof Rows) {
      Rows rows = (Rows) result;
      Statement<?> statement = (Statement<?>) executionInfo.getRequest();
      ColumnDefinitions columnDefinitions = getResultDefinitions(rows, statement, context);
      return new DefaultAsyncResultSet(
          columnDefinitions, executionInfo, rows.getData(), session, context);
    } else if (result instanceof Prepared) {
      // This should never happen
      throw new IllegalArgumentException("Unexpected PREPARED response to a CQL query");
    } else {
      // Void, SetKeyspace, SchemaChange
      return DefaultAsyncResultSet.empty(executionInfo);
    }
  }

  public static ColumnDefinitions getResultDefinitions(
      Rows rows, Statement<?> statement, InternalDriverContext context) {
    RowsMetadata rowsMetadata = rows.getMetadata();
    if (rowsMetadata.columnSpecs.isEmpty()) {
      // If the response has no metadata, it means the request had SKIP_METADATA set, the driver
      // only ever does that for bound statements.
      BoundStatement boundStatement = (BoundStatement) statement;
      return boundStatement.getPreparedStatement().getResultSetDefinitions();
    } else {
      // The response has metadata, always use it above anything else we might have locally.
      ColumnDefinitions definitions = toColumnDefinitions(rowsMetadata, context);
      // In addition, if the server signaled a schema change (see CASSANDRA-10786), update the
      // prepared statement's copy of the metadata
      if (rowsMetadata.newResultMetadataId != null) {
        BoundStatement boundStatement = (BoundStatement) statement;
        PreparedStatement preparedStatement = boundStatement.getPreparedStatement();
        preparedStatement.setResultMetadata(
            ByteBuffer.wrap(rowsMetadata.newResultMetadataId).asReadOnlyBuffer(), definitions);
      }
      return definitions;
    }
  }

  public static DefaultPreparedStatement toPreparedStatement(
      Prepared response, PrepareRequest request, InternalDriverContext context, LwtInfo lwtInfo) {
    ColumnDefinitions variableDefinitions =
        toColumnDefinitions(response.variablesMetadata, context);

    int[] pkIndicesInResponse = response.variablesMetadata.pkIndices;
    // null means a legacy protocol version that doesn't provide the info, try to compute it
    List<Integer> pkIndices =
        (pkIndicesInResponse == null)
            ? computePkIndices(variableDefinitions, context)
            : Ints.asList(pkIndicesInResponse);

    Partitioner partitioner = PartitionerFactory.partitioner(variableDefinitions, context);

    return new DefaultPreparedStatement(
        ByteBuffer.wrap(response.preparedQueryId).asReadOnlyBuffer(),
        request.getQuery(),
        variableDefinitions,
        pkIndices,
        (response.resultMetadataId == null)
            ? null
            : ByteBuffer.wrap(response.resultMetadataId).asReadOnlyBuffer(),
        toColumnDefinitions(response.resultMetadata, context),
        request.getKeyspace(),
        partitioner,
        NullAllowingImmutableMap.copyOf(request.getCustomPayload()),
        request.getExecutionProfileNameForBoundStatements(),
        request.getExecutionProfileForBoundStatements(),
        request.getRoutingKeyspaceForBoundStatements(),
        request.getRoutingKeyForBoundStatements(),
        request.getRoutingTokenForBoundStatements(),
        NullAllowingImmutableMap.copyOf(request.getCustomPayloadForBoundStatements()),
        request.areBoundStatementsIdempotent(),
        request.getTimeoutForBoundStatements(),
        request.getPagingStateForBoundStatements(),
        request.getPageSizeForBoundStatements(),
        request.getConsistencyLevelForBoundStatements(),
        request.getSerialConsistencyLevelForBoundStatements(),
        request.areBoundStatementsTracing(),
        context.getCodecRegistry(),
        context.getProtocolVersion(),
        lwtInfo != null && lwtInfo.isLwt(response.variablesMetadata.flags));
  }

  public static ColumnDefinitions toColumnDefinitions(
      RowsMetadata metadata, InternalDriverContext context) {
    ColumnDefinition[] values = new ColumnDefinition[metadata.columnSpecs.size()];
    int i = 0;
    for (ColumnSpec columnSpec : metadata.columnSpecs) {
      values[i++] = new DefaultColumnDefinition(columnSpec, context);
    }
    return DefaultColumnDefinitions.valueOf(ImmutableList.copyOf(values));
  }

  public static List<Integer> computePkIndices(
      ColumnDefinitions variables, InternalDriverContext context) {
    if (variables.size() == 0) {
      return Collections.emptyList();
    }
    // The rest of the computation relies on the fact that CQL does not have joins: all variables
    // belong to the same keyspace and table.
    ColumnDefinition firstVariable = variables.get(0);
    return context
        .getMetadataManager()
        .getMetadata()
        .getKeyspace(firstVariable.getKeyspace())
        .flatMap(ks -> ks.getTable(firstVariable.getTable()))
        .map(RelationMetadata::getPartitionKey)
        .map(pk -> findIndices(pk, variables))
        .orElse(Collections.emptyList());
  }

  // Find at which position in `variables` each element of `partitionKey` appears
  @VisibleForTesting
  static List<Integer> findIndices(List<ColumnMetadata> partitionKey, ColumnDefinitions variables) {
    ImmutableList.Builder<Integer> result =
        ImmutableList.builderWithExpectedSize(partitionKey.size());
    for (ColumnMetadata pkColumn : partitionKey) {
      int firstIndex = variables.firstIndexOf(pkColumn.getName());
      if (firstIndex < 0) {
        // If a single column is missing, we can abort right away
        return Collections.emptyList();
      } else {
        result.add(firstIndex);
      }
    }
    return result.build();
  }

  public static CoordinatorException toThrowable(
      Node node, Error errorMessage, InternalDriverContext context) {
    switch (errorMessage.code) {
      case ProtocolConstants.ErrorCode.UNPREPARED:
        throw new AssertionError(
            "UNPREPARED should be handled as a special case, not turned into an exception");
      case ProtocolConstants.ErrorCode.SERVER_ERROR:
        return new ServerError(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.PROTOCOL_ERROR:
        return new ProtocolError(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.AUTH_ERROR:
        // This method is used for query execution, authentication errors should only happen during
        // connection init
        return new ProtocolError(
            node, "Unexpected authentication error (" + errorMessage.message + ")");
      case ProtocolConstants.ErrorCode.UNAVAILABLE:
        Unavailable unavailable = (Unavailable) errorMessage;
        return new UnavailableException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(unavailable.consistencyLevel),
            unavailable.required,
            unavailable.alive);
      case ProtocolConstants.ErrorCode.OVERLOADED:
        return new OverloadedException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING:
        return new BootstrappingException(node);
      case ProtocolConstants.ErrorCode.TRUNCATE_ERROR:
        return new TruncateException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.WRITE_TIMEOUT:
        WriteTimeout writeTimeout = (WriteTimeout) errorMessage;
        return new WriteTimeoutException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(writeTimeout.consistencyLevel),
            writeTimeout.received,
            writeTimeout.blockFor,
            context.getWriteTypeRegistry().fromName(writeTimeout.writeType));
      case ProtocolConstants.ErrorCode.READ_TIMEOUT:
        ReadTimeout readTimeout = (ReadTimeout) errorMessage;
        return new ReadTimeoutException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(readTimeout.consistencyLevel),
            readTimeout.received,
            readTimeout.blockFor,
            readTimeout.dataPresent);
      case ProtocolConstants.ErrorCode.READ_FAILURE:
        ReadFailure readFailure = (ReadFailure) errorMessage;
        return new ReadFailureException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(readFailure.consistencyLevel),
            readFailure.received,
            readFailure.blockFor,
            readFailure.numFailures,
            readFailure.dataPresent,
            readFailure.reasonMap);
      case ProtocolConstants.ErrorCode.FUNCTION_FAILURE:
        return new FunctionFailureException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.WRITE_FAILURE:
        WriteFailure writeFailure = (WriteFailure) errorMessage;
        return new WriteFailureException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(writeFailure.consistencyLevel),
            writeFailure.received,
            writeFailure.blockFor,
            context.getWriteTypeRegistry().fromName(writeFailure.writeType),
            writeFailure.numFailures,
            writeFailure.reasonMap);
      case ProtocolConstants.ErrorCode.CDC_WRITE_FAILURE:
        return new CDCWriteFailureException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.CAS_WRITE_UNKNOWN:
        CASWriteUnknown casFailure = (CASWriteUnknown) errorMessage;
        return new CASWriteUnknownException(
            node,
            context.getConsistencyLevelRegistry().codeToLevel(casFailure.consistencyLevel),
            casFailure.received,
            casFailure.blockFor);
      case ProtocolConstants.ErrorCode.SYNTAX_ERROR:
        return new SyntaxError(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.UNAUTHORIZED:
        return new UnauthorizedException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.INVALID:
        return new InvalidQueryException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.CONFIG_ERROR:
        return new InvalidConfigurationInQueryException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.ALREADY_EXISTS:
        AlreadyExists alreadyExists = (AlreadyExists) errorMessage;
        return new AlreadyExistsException(node, alreadyExists.keyspace, alreadyExists.table);
      default:
        return new ProtocolError(node, "Unknown error code: " + errorMessage.code);
    }
  }

  public static boolean resolveIdempotence(Request request, InternalDriverContext context) {
    Boolean requestIsIdempotent = request.isIdempotent();
    DriverExecutionProfile executionProfile = resolveExecutionProfile(request, context);
    return (requestIsIdempotent == null)
        ? executionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)
        : requestIsIdempotent;
  }

  public static Duration resolveRequestTimeout(Request request, InternalDriverContext context) {
    DriverExecutionProfile executionProfile = resolveExecutionProfile(request, context);
    return request.getTimeout() != null
        ? request.getTimeout()
        : executionProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
  }

  public static RetryPolicy resolveRetryPolicy(Request request, InternalDriverContext context) {
    DriverExecutionProfile executionProfile = resolveExecutionProfile(request, context);
    return context.getRetryPolicy(executionProfile.getName());
  }

  public static SpeculativeExecutionPolicy resolveSpeculativeExecutionPolicy(
      Request request, InternalDriverContext context) {
    DriverExecutionProfile executionProfile = resolveExecutionProfile(request, context);
    return context.getSpeculativeExecutionPolicy(executionProfile.getName());
  }
}
