/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.WriteType;
import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
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
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods to convert to/from protocol messages.
 *
 * <p>The main goal of this class is to move this code out of the request handlers.
 */
class Conversions {

  static Message toMessage(
      Statement statement, DriverConfigProfile config, InternalDriverContext context) {
    if (statement instanceof SimpleStatement) {
      SimpleStatement simpleStatement = (SimpleStatement) statement;

      CodecRegistry codecRegistry = context.codecRegistry();
      List<Object> values = simpleStatement.getValues();
      List<ByteBuffer> encodedValues = new ArrayList<>(values.size());
      for (Object value : values) {
        encodedValues.add(codecRegistry.codecFor(value).encode(value, context.protocolVersion()));
      }
      int consistency =
          config.getConsistencyLevel(CoreDriverOption.REQUEST_CONSISTENCY).getProtocolCode();
      boolean skipMetadata = false; // TODO set for bound statements
      int pageSize = config.getInt(CoreDriverOption.REQUEST_PAGE_SIZE);
      ByteBuffer pagingState = null; // TODO paging
      int serialConsistency =
          config.getConsistencyLevel(CoreDriverOption.REQUEST_SERIAL_CONSISTENCY).getProtocolCode();
      long timestamp = Long.MIN_VALUE; // TODO timestamp generator
      QueryOptions queryOptions =
          new QueryOptions(
              consistency,
              encodedValues,
              Collections.emptyMap(),
              skipMetadata,
              pageSize,
              pagingState,
              serialConsistency,
              timestamp);
      return new Query(simpleStatement.getQuery(), queryOptions);
    }
    // TODO handle other types of statements
    throw new UnsupportedOperationException("TODO handle other types of statements");
  }

  static AsyncResultSet toResultSet(
      Result result, ExecutionInfo executionInfo, InternalDriverContext context) {
    if (result instanceof Rows) {
      Rows rows = (Rows) result;
      ImmutableList.Builder<ColumnDefinition> definitions = ImmutableList.builder();
      for (ColumnSpec columnSpec : rows.metadata.columnSpecs) {
        definitions.add(new DefaultColumnDefinition(columnSpec, context));
      }
      return new DefaultAsyncResultSet(
          new DefaultColumnDefinitions(definitions.build()), executionInfo, rows.data, context);
    } else if (result instanceof Prepared) {
      // This should never happen
      throw new IllegalArgumentException("Unexpected PREPARED response to a CQL query");
    } else {
      // Void, SetKeyspace, SchemaChange
      return DefaultAsyncResultSet.empty(executionInfo);
    }
  }

  static Throwable toThrowable(Node node, Error errorMessage) {
    switch (errorMessage.code) {
      case ProtocolConstants.ErrorCode.UNPREPARED:
        throw new AssertionError(
            "UNPREPARED should be handled as a special case, not turned into an exception");
      case ProtocolConstants.ErrorCode.SERVER_ERROR:
        return new ServerError(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.PROTOCOL_ERROR:
        return new ProtocolError(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.AUTH_ERROR:
        // This method is used for query execution, authentication errors are unlikely to happen at
        // this stage (more during connection init), but there's no harm in handling them anyway
        return new AuthenticationException(node.getConnectAddress(), errorMessage.message);
      case ProtocolConstants.ErrorCode.UNAVAILABLE:
        Unavailable unavailable = (Unavailable) errorMessage;
        return new UnavailableException(
            node,
            ConsistencyLevel.fromCode(unavailable.consistencyLevel),
            unavailable.required,
            unavailable.alive);
      case ProtocolConstants.ErrorCode.OVERLOADED:
        return new OverloadedException(node);
      case ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING:
        return new BootstrappingException(node);
      case ProtocolConstants.ErrorCode.TRUNCATE_ERROR:
        return new TruncateException(node, errorMessage.message);
      case ProtocolConstants.ErrorCode.WRITE_TIMEOUT:
        WriteTimeout writeTimeout = (WriteTimeout) errorMessage;
        return new WriteTimeoutException(
            node,
            ConsistencyLevel.fromCode(writeTimeout.consistencyLevel),
            writeTimeout.received,
            writeTimeout.blockFor,
            WriteType.valueOf(writeTimeout.writeType));
      case ProtocolConstants.ErrorCode.READ_TIMEOUT:
        ReadTimeout readTimeout = (ReadTimeout) errorMessage;
        return new ReadTimeoutException(
            node,
            ConsistencyLevel.fromCode(readTimeout.consistencyLevel),
            readTimeout.received,
            readTimeout.blockFor,
            readTimeout.dataPresent);
      case ProtocolConstants.ErrorCode.READ_FAILURE:
        ReadFailure readFailure = (ReadFailure) errorMessage;
        return new ReadFailureException(
            node,
            ConsistencyLevel.fromCode(readFailure.consistencyLevel),
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
            ConsistencyLevel.fromCode(writeFailure.consistencyLevel),
            writeFailure.received,
            writeFailure.blockFor,
            WriteType.valueOf(writeFailure.writeType),
            writeFailure.numFailures,
            writeFailure.reasonMap);
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
}
