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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.session.Request;
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
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import io.netty.util.concurrent.EventExecutor;
import java.util.Map;
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

  public CqlPrepareAsyncProcessor(DefaultDriverContext context) {
    this(CacheBuilder.newBuilder().weakValues().build(), context);
  }

  protected CqlPrepareAsyncProcessor(
      Cache<PrepareRequest, CompletableFuture<PreparedStatement>> cache,
      DefaultDriverContext context) {

    this.cache = cache;

    EventExecutor adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    context
        .getEventBus()
        .register(TypeChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onTypeChanged));
  }

  private static boolean typeMatches(UserDefinedType oldType, ColumnDefinition def) {
    if (def.getType().getProtocolCode() != ProtocolConstants.DataType.UDT) return false;
    UserDefinedType defType = (UserDefinedType) def.getType();
    return defType.equals(oldType);
  }

  private void onTypeChanged(TypeChangeEvent event) {
    LOG.info("newType from event: " + event.newType);
    LOG.info("oldType from event: " + event.oldType);
    for (Map.Entry<PrepareRequest, CompletableFuture<PreparedStatement>> entry :
        this.cache.asMap().entrySet()) {

      try {
        PreparedStatement stmt = entry.getValue().get();
        if (Streams.stream(stmt.getResultSetDefinitions())
                .anyMatch(def -> typeMatches(event.oldType, def))
            || Streams.stream(stmt.getVariableDefinitions())
                .anyMatch(def -> typeMatches(event.oldType, def))) {
          this.cache.invalidate(entry.getKey());
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
