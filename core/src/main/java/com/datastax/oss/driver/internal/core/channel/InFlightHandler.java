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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.RequestMessage;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.SetKeyspaceEvent;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.collect.BiMap;
import com.datastax.oss.driver.shaded.guava.common.collect.HashBiMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages requests that are currently executing on a channel. */
@NotThreadSafe
public class InFlightHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InFlightHandler.class);

  private final ProtocolVersion protocolVersion;
  private final StreamIdGenerator streamIds;
  final ChannelPromise closeStartedFuture;
  private final String ownerLogPrefix;
  private final BiMap<Integer, ResponseCallback> inFlight;
  private final Map<Integer, ResponseCallback> orphaned;
  private volatile int orphanedSize; // thread-safe view for metrics
  private final long setKeyspaceTimeoutMillis;
  private final EventCallback eventCallback;
  private final int maxOrphanStreamIds;
  private boolean closingGracefully;
  private SetKeyspaceRequest setKeyspaceRequest;
  private String logPrefix;

  InFlightHandler(
      ProtocolVersion protocolVersion,
      StreamIdGenerator streamIds,
      int maxOrphanStreamIds,
      long setKeyspaceTimeoutMillis,
      ChannelPromise closeStartedFuture,
      EventCallback eventCallback,
      String ownerLogPrefix) {
    this.protocolVersion = protocolVersion;
    this.streamIds = streamIds;
    this.maxOrphanStreamIds = maxOrphanStreamIds;
    this.closeStartedFuture = closeStartedFuture;
    this.ownerLogPrefix = ownerLogPrefix;
    this.logPrefix = ownerLogPrefix + "|connecting...";
    this.inFlight = HashBiMap.create(streamIds.getMaxAvailableIds());
    this.orphaned = new HashMap<>(maxOrphanStreamIds);
    this.setKeyspaceTimeoutMillis = setKeyspaceTimeoutMillis;
    this.eventCallback = eventCallback;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    super.channelActive(ctx);
    String channelId = ctx.channel().toString();
    this.logPrefix = ownerLogPrefix + "|" + channelId.substring(1, channelId.length() - 1);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object in, ChannelPromise promise) throws Exception {
    if (in == DriverChannel.GRACEFUL_CLOSE_MESSAGE) {
      LOG.debug("[{}] Received graceful close request", logPrefix);
      startGracefulShutdown(ctx);
    } else if (in == DriverChannel.FORCEFUL_CLOSE_MESSAGE) {
      LOG.debug("[{}] Received forceful close request, aborting pending queries", logPrefix);
      abortAllInFlight(new ClosedConnectionException("Channel was force-closed"));
      ctx.channel().close();
    } else if (in instanceof HeartbeatException) {
      abortAllInFlight(
          new ClosedConnectionException("Heartbeat query failed", ((HeartbeatException) in)));
      ctx.close();
    } else if (in instanceof RequestMessage) {
      write(ctx, (RequestMessage) in, promise);
    } else if (in instanceof ResponseCallback) {
      cancel(ctx, (ResponseCallback) in, promise);
    } else {
      promise.setFailure(
          new IllegalArgumentException("Unsupported message type " + in.getClass().getName()));
    }
  }

  private void write(ChannelHandlerContext ctx, RequestMessage message, ChannelPromise promise) {
    if (closingGracefully) {
      promise.setFailure(new IllegalStateException("Channel is closing"));
      streamIds.cancelPreAcquire();
      return;
    }
    int streamId = streamIds.acquire();
    if (streamId < 0) {
      // Should not happen with the preAcquire mechanism, but handle gracefully
      promise.setFailure(
          new BusyConnectionException(
              String.format(
                  "Couldn't acquire a stream id from InFlightHandler on %s", ctx.channel())));
      streamIds.cancelPreAcquire();
      return;
    }

    if (inFlight.containsKey(streamId)) {
      promise.setFailure(
          new IllegalStateException("Found pending callback for stream id " + streamId));
      streamIds.cancelPreAcquire();
      return;
    }

    LOG.trace("[{}] Writing {} on stream id {}", logPrefix, message.responseCallback, streamId);
    Frame frame =
        Frame.forRequest(
            protocolVersion.getCode(),
            streamId,
            message.tracing,
            message.customPayload,
            message.request);

    inFlight.put(streamId, message.responseCallback);
    ChannelFuture writeFuture = ctx.write(frame, promise);
    writeFuture.addListener(
        future -> {
          if (future.isSuccess()) {
            message.responseCallback.onStreamIdAssigned(streamId);
          } else {
            release(streamId, ctx);
          }
        });
  }

  private void cancel(
      ChannelHandlerContext ctx, ResponseCallback responseCallback, ChannelPromise promise) {
    Integer streamId = inFlight.inverse().remove(responseCallback);
    if (streamId == null) {
      LOG.trace(
          "[{}] Received cancellation for unknown or already cancelled callback {}, skipping",
          logPrefix,
          responseCallback);
    } else {
      LOG.trace(
          "[{}] Cancelled callback {} for stream id {}", logPrefix, responseCallback, streamId);
      if (closingGracefully && inFlight.isEmpty()) {
        LOG.debug("[{}] Last pending query was cancelled, closing channel", logPrefix);
        ctx.channel().close();
      } else {
        // We can't release the stream id, because a response might still come back from the server.
        // Keep track of those "orphaned" ids, to release them later if we get a response and the
        // callback says it's the last one.
        orphaned.put(streamId, responseCallback);
        if (orphaned.size() > maxOrphanStreamIds) {
          LOG.debug(
              "[{}] Orphan stream ids exceeded the configured threshold ({}), closing gracefully",
              logPrefix,
              maxOrphanStreamIds);
          startGracefulShutdown(ctx);
        } else {
          orphanedSize = orphaned.size();
        }
      }
    }
    promise.setSuccess();
  }

  private void startGracefulShutdown(ChannelHandlerContext ctx) {
    if (inFlight.isEmpty()) {
      LOG.debug("[{}] No pending queries, completing graceful shutdown now", logPrefix);
      ctx.channel().close();
    } else {
      // remove heartbeat handler from pipeline if present.
      ChannelHandler heartbeatHandler = ctx.pipeline().get(ChannelFactory.HEARTBEAT_HANDLER_NAME);
      if (heartbeatHandler != null) {
        ctx.pipeline().remove(heartbeatHandler);
      }
      LOG.debug("[{}] There are pending queries, delaying graceful shutdown", logPrefix);
      closingGracefully = true;
      closeStartedFuture.setSuccess();
    }
  }

  @Override
  @SuppressWarnings("NonAtomicVolatileUpdate")
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Frame responseFrame = (Frame) msg;
    int streamId = responseFrame.streamId;

    if (streamId < 0) {
      Message event = responseFrame.message;
      if (eventCallback == null) {
        LOG.debug("[{}] Received event {} but no callback was registered", logPrefix, event);
      } else {
        LOG.debug("[{}] Received event {}, notifying callback", logPrefix, event);
        try {
          eventCallback.onEvent(event);
        } catch (Throwable t) {
          Loggers.warnWithException(
              LOG, "[{}] Unexpected error while invoking event handler", logPrefix, t);
        }
      }
    } else {
      boolean wasInFlight = true;
      ResponseCallback callback = inFlight.get(streamId);
      if (callback == null) {
        wasInFlight = false;
        callback = orphaned.get(streamId);
        if (callback == null) {
          LOG.trace("[{}] Got response on unknown stream id {}, skipping", logPrefix, streamId);
          return;
        }
      }
      try {
        if (callback.isLastResponse(responseFrame)) {
          LOG.debug(
              "[{}] Got last response on {} stream id {}, completing and releasing",
              logPrefix,
              wasInFlight ? "in-flight" : "orphaned",
              streamId);
          release(streamId, ctx);
        } else {
          LOG.trace(
              "[{}] Got non-last response on {} stream id {}, still holding",
              logPrefix,
              wasInFlight ? "in-flight" : "orphaned",
              streamId);
        }
        if (wasInFlight) {
          callback.onResponse(responseFrame);
        }
      } catch (Throwable t) {
        if (wasInFlight) {
          fail(
              callback,
              new IllegalArgumentException("Unexpected error while invoking response handler", t));
        } else {
          // Assume the callback is already completed, so it's better to log
          Loggers.warnWithException(
              LOG,
              "[{}] Unexpected error while invoking response handler on stream id {}",
              logPrefix,
              t,
              streamId);
        }
      }
    }
  }

  /** Called if an exception was thrown while processing an inbound event (i.e. a response). */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable exception) throws Exception {
    if (exception instanceof FrameDecodingException) {
      int streamId = ((FrameDecodingException) exception).streamId;
      LOG.debug("[{}] Error while decoding response on stream id {}", logPrefix, streamId);
      if (streamId >= 0) {
        // We know which request matches the failing response, fail that one only
        ResponseCallback responseCallback = inFlight.get(streamId);
        if (responseCallback != null) {
          fail(responseCallback, exception.getCause());
        }
        release(streamId, ctx);
      } else {
        Loggers.warnWithException(
            LOG,
            "[{}] Unexpected error while decoding incoming event frame",
            logPrefix,
            exception.getCause());
      }
    } else {
      // Otherwise fail all pending requests
      abortAllInFlight(
          (exception instanceof HeartbeatException)
              ? (HeartbeatException) exception
              : new ClosedConnectionException("Unexpected error on channel", exception));
      ctx.close();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
    if (event instanceof SetKeyspaceEvent) {
      SetKeyspaceEvent setKeyspaceEvent = (SetKeyspaceEvent) event;
      if (this.setKeyspaceRequest != null) {
        setKeyspaceEvent.promise.setFailure(
            new IllegalStateException(
                "Can't call setKeyspace while a keyspace switch is already in progress"));
      } else {
        LOG.debug(
            "[{}] Switching to keyspace {}", logPrefix, setKeyspaceEvent.keyspaceName.asInternal());
        this.setKeyspaceRequest = new SetKeyspaceRequest(ctx, setKeyspaceEvent);
        this.setKeyspaceRequest.send();
      }
    } else {
      super.userEventTriggered(ctx, event);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    // If the channel was closed normally (normal or forced shutdown), inFlight is already empty by
    // the time we get here. So if it's not, it means the channel closed unexpectedly (e.g. the
    // connection was dropped).
    abortAllInFlight(new ClosedConnectionException("Lost connection to remote peer"));
    super.channelInactive(ctx);
  }

  private void release(int streamId, ChannelHandlerContext ctx) {
    LOG.trace("[{}] Releasing stream id {}", logPrefix, streamId);
    if (inFlight.remove(streamId) != null) {
      // If we're in the middle of an orderly close and this was the last request, actually close
      // the channel now
      if (closingGracefully && inFlight.isEmpty()) {
        LOG.debug("[{}] Done handling the last pending query, closing channel", logPrefix);
        ctx.channel().close();
      }
    } else if (orphaned.remove(streamId) != null) {
      orphanedSize = orphaned.size();
    }
    // Note: it's possible that the callback is in neither map, if we get here after a call to
    // abortAllInFlight that already cleared the map (see JAVA-2000)
    streamIds.release(streamId);
  }

  private void abortAllInFlight(DriverException cause) {
    abortAllInFlight(cause, null);
  }

  /**
   * @param ignore the ResponseCallback that called this method, if applicable (avoids a recursive
   *     loop)
   */
  private void abortAllInFlight(DriverException cause, ResponseCallback ignore) {
    if (!inFlight.isEmpty()) {

      // Create a local copy and clear the map immediately. This prevents
      // ConcurrentModificationException if aborting one of the handlers recurses back into this
      // method.
      Set<ResponseCallback> responseCallbacks = ImmutableSet.copyOf(inFlight.values());
      inFlight.clear();

      for (ResponseCallback responseCallback : responseCallbacks) {
        if (responseCallback != ignore) {
          fail(responseCallback, cause);
        }
      }
      // It's not necessary to release the stream ids, since we always call this method right before
      // closing the channel
    }
  }

  private void fail(ResponseCallback callback, Throwable failure) {
    try {
      callback.onFailure(failure);
    } catch (Throwable throwable) {
      // Protect against unexpected errors. We don't have anywhere to report the error (since
      // onFailure failed), so log as a last resort.
      LOG.error("[{}] Unexpected error while failing {}", logPrefix, callback, throwable);
    }
  }

  int getAvailableIds() {
    return streamIds.getAvailableIds();
  }

  boolean preAcquireId() {
    return streamIds.preAcquire();
  }

  int getInFlight() {
    return streamIds.getMaxAvailableIds() - streamIds.getAvailableIds();
  }

  int getOrphanIds() {
    return orphanedSize;
  }

  private class SetKeyspaceRequest extends ChannelHandlerRequest {

    private final CqlIdentifier keyspaceName;
    private final Promise<Void> promise;

    SetKeyspaceRequest(ChannelHandlerContext ctx, SetKeyspaceEvent setKeyspaceEvent) {
      super(ctx, setKeyspaceTimeoutMillis);
      this.keyspaceName = setKeyspaceEvent.keyspaceName;
      this.promise = setKeyspaceEvent.promise;
    }

    @Override
    String describe() {
      return "[" + logPrefix + "] Set keyspace request (USE " + keyspaceName.asCql(true) + ")";
    }

    @Override
    Message getRequest() {
      return new Query("USE " + keyspaceName.asCql(false));
    }

    @Override
    void onResponse(Message response) {
      if (response instanceof SetKeyspace) {
        if (promise.trySuccess(null)) {
          InFlightHandler.this.setKeyspaceRequest = null;
        }
      } else {
        failOnUnexpected(response);
      }
    }

    @Override
    void fail(String message, Throwable cause) {
      ClosedConnectionException setKeyspaceException =
          new ClosedConnectionException(message, cause);
      if (promise.tryFailure(setKeyspaceException)) {
        InFlightHandler.this.setKeyspaceRequest = null;
        // setKeyspace queries are not triggered directly by the user, but only as a response to a
        // successful "USE... query", so the keyspace name should generally be valid. If the
        // keyspace switch fails, this could be due to a schema disagreement or a more serious
        // error. Rescheduling the switch is impractical, we can't do much better than closing the
        // channel and letting it reconnect.
        Loggers.warnWithException(
            LOG, "[{}] Unexpected error while switching keyspace", logPrefix, setKeyspaceException);
        abortAllInFlight(setKeyspaceException, this);
        ctx.channel().close();
      }
    }
  }
}
