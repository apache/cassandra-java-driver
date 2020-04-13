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
package com.datastax.dse.driver.internal.core.cql;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.servererrors.UnfitClientException;
import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.dse.protocol.internal.request.query.ContinuousPagingOptions;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DseConversions {

  public static Message toContinuousPagingMessage(
      Statement<?> statement, DriverExecutionProfile config, InternalDriverContext context) {
    ConsistencyLevelRegistry consistencyLevelRegistry = context.getConsistencyLevelRegistry();
    ConsistencyLevel consistency = statement.getConsistencyLevel();
    int consistencyCode =
        (consistency == null)
            ? consistencyLevelRegistry.nameToCode(
                config.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
            : consistency.getProtocolCode();
    int pageSize = config.getInt(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE);
    boolean pageSizeInBytes = config.getBoolean(DseDriverOption.CONTINUOUS_PAGING_PAGE_SIZE_BYTES);
    int maxPages = config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES);
    int maxPagesPerSecond = config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND);
    int maxEnqueuedPages = config.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES);
    ContinuousPagingOptions options =
        new ContinuousPagingOptions(maxPages, maxPagesPerSecond, maxEnqueuedPages);
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
      DseQueryOptions queryOptions =
          new DseQueryOptions(
              consistencyCode,
              Conversions.encode(positionalValues, codecRegistry, protocolVersion),
              Conversions.encode(namedValues, codecRegistry, protocolVersion),
              false,
              pageSize,
              statement.getPagingState(),
              serialConsistencyCode,
              timestamp,
              (keyspace == null) ? null : keyspace.asInternal(),
              pageSizeInBytes,
              options);
      return new Query(simpleStatement.getQuery(), queryOptions);
    } else if (statement instanceof BoundStatement) {
      BoundStatement boundStatement = (BoundStatement) statement;
      if (!protocolVersionRegistry.supports(
          protocolVersion, DefaultProtocolFeature.UNSET_BOUND_VALUES)) {
        Conversions.ensureAllSet(boundStatement);
      }
      boolean skipMetadata =
          boundStatement.getPreparedStatement().getResultSetDefinitions().size() > 0;
      DseQueryOptions queryOptions =
          new DseQueryOptions(
              consistencyCode,
              boundStatement.getValues(),
              Collections.emptyMap(),
              skipMetadata,
              pageSize,
              statement.getPagingState(),
              serialConsistencyCode,
              timestamp,
              null,
              pageSizeInBytes,
              options);
      PreparedStatement preparedStatement = boundStatement.getPreparedStatement();
      ByteBuffer id = preparedStatement.getId();
      ByteBuffer resultMetadataId = preparedStatement.getResultMetadataId();
      return new Execute(
          Bytes.getArray(id),
          (resultMetadataId == null) ? null : Bytes.getArray(resultMetadataId),
          queryOptions);
    } else {
      throw new IllegalArgumentException(
          "Unsupported statement type: " + statement.getClass().getName());
    }
  }

  public static CoordinatorException toThrowable(
      Node node, Error errorMessage, InternalDriverContext context) {
    switch (errorMessage.code) {
      case DseProtocolConstants.ErrorCode.CLIENT_WRITE_FAILURE:
        return new UnfitClientException(node, errorMessage.message);
      default:
        return Conversions.toThrowable(node, errorMessage, context);
    }
  }
}
