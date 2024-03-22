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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.schema.events.TypeChangeEvent;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import com.datastax.oss.driver.shaded.guava.common.cache.CacheBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class CqlPrepareAsyncProcessor
    implements RequestProcessor<PrepareRequest, CompletionStage<PreparedStatement>> {

  private static final Logger LOG = LoggerFactory.getLogger(CqlPrepareAsyncProcessor.class);

  protected final Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache;

  public CqlPrepareAsyncProcessor() {
    this(Optional.empty());
  }

  public CqlPrepareAsyncProcessor(@NonNull Optional<? extends DefaultDriverContext> context) {
    this(CacheBuilder.newBuilder().weakValues().build(), context);
  }

  protected CqlPrepareAsyncProcessor(
      Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache,
      Optional<? extends DefaultDriverContext> context) {

    this.cache = cache;
    context.ifPresent(
        (ctx) -> {
          LOG.info("Adding handler to invalidate cached prepared statements on type changes");
          EventExecutor adminExecutor = ctx.getNettyOptions().adminEventExecutorGroup().next();
          ctx.getEventBus()
              .register(
                  TypeChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onTypeChanged));
        });
  }

  private static boolean typeMatches(UserDefinedType oldType, DataType typeToCheck) {

    switch (typeToCheck.getProtocolCode()) {
      case ProtocolConstants.DataType.UDT:
        UserDefinedType udtType = (UserDefinedType) typeToCheck;
        return udtType.equals(oldType)
            ? true
            : Iterables.any(udtType.getFieldTypes(), (testType) -> typeMatches(oldType, testType));
      case ProtocolConstants.DataType.LIST:
        ListType listType = (ListType) typeToCheck;
        return typeMatches(oldType, listType.getElementType());
      case ProtocolConstants.DataType.SET:
        SetType setType = (SetType) typeToCheck;
        return typeMatches(oldType, setType.getElementType());
      case ProtocolConstants.DataType.MAP:
        MapType mapType = (MapType) typeToCheck;
        return typeMatches(oldType, mapType.getKeyType())
            || typeMatches(oldType, mapType.getValueType());
      case ProtocolConstants.DataType.TUPLE:
        TupleType tupleType = (TupleType) typeToCheck;
        return Iterables.any(
            tupleType.getComponentTypes(), (testType) -> typeMatches(oldType, testType));
      default:
        return false;
    }
  }

  private void onTypeChanged(TypeChangeEvent event) {
    for (Map.Entry<PrepareRequest, CompletableFuture<PreparedStatement>> entry :
        this.cache.asMap().entrySet()) {

      try {
        PreparedStatement stmt = entry.getValue().get();
        if (Iterables.any(
                stmt.getResultSetDefinitions(), (def) -> typeMatches(event.oldType, def.getType()))
            || Iterables.any(
                stmt.getVariableDefinitions(),
                (def) -> typeMatches(event.oldType, def.getType()))) {

          this.cache.invalidate(entry.getKey());
          this.cache.cleanUp();
        }
      } catch (Exception e) {
        LOG.info("Exception while invalidating prepared statement cache due to UDT change", e);
      }
    }
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof PrepareRequest && resultType.equals(PrepareRequest.ASYNC);
  }

  @Override
  public CompletionStage<PreparedStatement> process(
      PrepareRequest request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    try {
      CompletableFuture<PreparedStatement> result = cache.getIfPresent(request);
      if (result == null) {
        CompletableFuture<PreparedStatement> mine = new CompletableFuture<>();
        result = cache.get(request, () -> mine);
        if (result == mine) {
          new CqlPrepareHandler(request, session, context, sessionLogPrefix)
              .handle()
              .whenComplete(
                  (preparedStatement, error) -> {
                    if (error != null) {
                      mine.completeExceptionally(error);
                      cache.invalidate(request); // Make sure failure isn't cached indefinitely
                    } else if (mine.isCancelled()) {
                      cache.invalidate(request);
                      process(request, session, context, sessionLogPrefix);
                    } else {
                      mine.complete(preparedStatement);
                    }
                  });
        }
      }
      return result;
    } catch (ExecutionException e) {
      return CompletableFutures.failedFuture(e.getCause());
    }
  }

  @Override
  public CompletionStage<PreparedStatement> newFailure(RuntimeException error) {
    return CompletableFutures.failedFuture(error);
  }

  public Cache<PrepareRequest, CompletableFuture<PreparedStatement>> getCache() {
    return cache;
  }
}
