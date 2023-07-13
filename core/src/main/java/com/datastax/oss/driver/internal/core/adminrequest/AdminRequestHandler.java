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
package com.datastax.oss.driver.internal.core.adminrequest;

import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.Rows;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.ScheduledFuture;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the lifecyle of an admin request (such as a node refresh or schema refresh query). */
@ThreadSafe
public class AdminRequestHandler implements ResponseCallback {
  private static final Logger LOG = LoggerFactory.getLogger(AdminRequestHandler.class);

  public static AdminRequestHandler query(
      DriverChannel channel, Query query, Duration timeout, String logPrefix) {
    return createAdminRequestHandler(channel, query, Collections.emptyMap(), timeout, logPrefix);
  }

  public static AdminRequestHandler query(
      DriverChannel channel,
      String query,
      Map<String, Object> parameters,
      Duration timeout,
      int pageSize,
      String logPrefix) {
    Query message =
        new Query(
            query,
            buildQueryOptions(pageSize, serialize(parameters, channel.protocolVersion()), null));
    return createAdminRequestHandler(channel, message, parameters, timeout, logPrefix);
  }

  private static AdminRequestHandler createAdminRequestHandler(
      DriverChannel channel,
      Query message,
      Map<String, Object> parameters,
      Duration timeout,
      String logPrefix) {

    String debugString = "query '" + message.query + "'";
    if (!parameters.isEmpty()) {
      debugString += " with parameters " + parameters;
    }
    return new AdminRequestHandler(
        channel, message, Frame.NO_PAYLOAD, timeout, logPrefix, debugString);
  }

  public static AdminRequestHandler query(
      DriverChannel channel, String query, Duration timeout, int pageSize, String logPrefix) {
    return query(channel, query, Collections.emptyMap(), timeout, pageSize, logPrefix);
  }

  private final DriverChannel channel;
  private final Message message;
  private final Map<String, ByteBuffer> customPayload;
  private final Duration timeout;
  private final String logPrefix;
  private final String debugString;
  protected final CompletableFuture<AdminResult> result = new CompletableFuture<>();

  // This is only ever accessed on the channel's event loop, so it doesn't need to be volatile
  private ScheduledFuture<?> timeoutFuture;

  public AdminRequestHandler(
      DriverChannel channel,
      Message message,
      Map<String, ByteBuffer> customPayload,
      Duration timeout,
      String logPrefix,
      String debugString) {
    this.channel = channel;
    this.message = message;
    this.customPayload = customPayload;
    this.timeout = timeout;
    this.logPrefix = logPrefix;
    this.debugString = debugString;
  }

  public CompletionStage<AdminResult> start() {
    LOG.debug("[{}] Executing {}", logPrefix, this);
    channel.write(message, false, customPayload, this).addListener(this::onWriteComplete);
    return result;
  }

  private void onWriteComplete(Future<? super Void> future) {
    if (future.isSuccess()) {
      LOG.debug("[{}] Successfully wrote {}, waiting for response", logPrefix, this);
      if (timeout.toNanos() > 0) {
        timeoutFuture =
            channel
                .eventLoop()
                .schedule(this::fireTimeout, timeout.toNanos(), TimeUnit.NANOSECONDS);
        timeoutFuture.addListener(UncaughtExceptions::log);
      }
    } else {
      setFinalError(future.cause());
    }
  }

  private void fireTimeout() {
    setFinalError(
        new DriverTimeoutException(String.format("%s timed out after %s", debugString, timeout)));
    if (!channel.closeFuture().isDone()) {
      channel.cancel(this);
    }
  }

  @Override
  public void onFailure(Throwable error) {
    if (timeoutFuture != null) {
      timeoutFuture.cancel(true);
    }
    setFinalError(error);
  }

  @Override
  public void onResponse(Frame responseFrame) {
    if (timeoutFuture != null) {
      timeoutFuture.cancel(true);
    }
    Message message = responseFrame.message;
    LOG.debug("[{}] Got response {}", logPrefix, responseFrame.message);
    if (message instanceof Rows) {
      Rows rows = (Rows) message;
      ByteBuffer pagingState = rows.getMetadata().pagingState;
      AdminRequestHandler nextHandler = (pagingState == null) ? null : this.copy(pagingState);
      setFinalResult(new AdminResult(rows, nextHandler, channel.protocolVersion()));
    } else if (message instanceof Result) {

      // Internal prepares are only "reprepare on up" types of queries, where we only care about
      // success, not the actual result, so this is good enough:
      setFinalResult(null);
    } else {
      setFinalError(new UnexpectedResponseException(debugString, message));
    }
  }

  protected boolean setFinalResult(AdminResult result) {
    return this.result.complete(result);
  }

  protected boolean setFinalError(Throwable error) {
    return result.completeExceptionally(error);
  }

  private AdminRequestHandler copy(ByteBuffer pagingState) {
    assert message instanceof Query;
    Query current = (Query) this.message;
    QueryOptions currentOptions = current.options;
    QueryOptions newOptions =
        buildQueryOptions(currentOptions.pageSize, currentOptions.namedValues, pagingState);
    return new AdminRequestHandler(
        channel,
        new Query(current.query, newOptions),
        customPayload,
        timeout,
        logPrefix,
        debugString);
  }

  private static QueryOptions buildQueryOptions(
      int pageSize, Map<String, ByteBuffer> serialize, ByteBuffer pagingState) {
    return new QueryOptions(
        ProtocolConstants.ConsistencyLevel.ONE,
        Collections.emptyList(),
        serialize,
        false,
        pageSize,
        pagingState,
        ProtocolConstants.ConsistencyLevel.SERIAL,
        Long.MIN_VALUE,
        null);
  }

  private static Map<String, ByteBuffer> serialize(
      Map<String, Object> parameters, ProtocolVersion protocolVersion) {
    Map<String, ByteBuffer> result = Maps.newHashMapWithExpectedSize(parameters.size());
    for (Map.Entry<String, Object> entry : parameters.entrySet()) {
      result.put(entry.getKey(), serialize(entry.getValue(), protocolVersion));
    }
    return result;
  }

  private static ByteBuffer serialize(Object parameter, ProtocolVersion protocolVersion) {
    if (parameter instanceof String) {
      return TypeCodecs.TEXT.encode((String) parameter, protocolVersion);
    } else if (parameter instanceof InetAddress) {
      return TypeCodecs.INET.encode((InetAddress) parameter, protocolVersion);
    } else if (parameter instanceof List && ((List) parameter).get(0) instanceof String) {
      @SuppressWarnings("unchecked")
      List<String> l = (List<String>) parameter;
      return AdminRow.LIST_OF_TEXT.encode(l, protocolVersion);
    } else {
      throw new IllegalArgumentException(
          "Unsupported variable type for admin query: " + parameter.getClass());
    }
  }

  @Override
  public String toString() {
    return debugString;
  }
}
