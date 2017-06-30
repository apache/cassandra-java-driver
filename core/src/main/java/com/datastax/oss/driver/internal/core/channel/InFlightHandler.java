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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.ReleaseEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.RequestMessage;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.SetKeyspaceEvent;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages requests that are currently executing on a channel. */
public class InFlightHandler extends ChannelDuplexHandler {
  private static final Logger LOG = LoggerFactory.getLogger(InFlightHandler.class);

  private final ProtocolVersion protocolVersion;
  private final StreamIdGenerator streamIds;
  final ChannelPromise closeStartedFuture;
  private final String ownerLogPrefix;
  private final BiMap<Integer, ResponseCallback> inFlight;
  private final long setKeyspaceTimeoutMillis;
  private final AvailableIdsHolder availableIdsHolder;
  private final EventCallback eventCallback;
  private final int maxOrphanStreamIds;
  private boolean closingGracefully;
  private SetKeyspaceRequest setKeyspaceRequest;
  private String logPrefix;
  private int orphanStreamIds;

  InFlightHandler(
      ProtocolVersion protocolVersion,
      StreamIdGenerator streamIds,
      int maxOrphanStreamIds,
      long setKeyspaceTimeoutMillis,
      AvailableIdsHolder availableIdsHolder,
      ChannelPromise closeStartedFuture,
      EventCallback eventCallback,
      String ownerLogPrefix) {
    this.protocolVersion = protocolVersion;
    this.streamIds = streamIds;
    this.maxOrphanStreamIds = maxOrphanStreamIds;
    this.closeStartedFuture = closeStartedFuture;
    this.ownerLogPrefix = ownerLogPrefix;
    this.logPrefix = ownerLogPrefix + "|connecting...";
    reportAvailableIds();
    this.inFlight = HashBiMap.create(streamIds.getMaxAvailableIds());
    this.setKeyspaceTimeoutMillis = setKeyspaceTimeoutMillis;
    this.availableIdsHolder = availableIdsHolder;
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
      return;
    }
    int streamId = streamIds.acquire();
    if (streamId < 0) {
      promise.setFailure(new BusyConnectionException(streamIds.getMaxAvailableIds()));
      return;
    }

    if (inFlight.containsKey(streamId)) {
      promise.setFailure(
          new IllegalStateException("Found pending callback for stream id " + streamId));
      return;
    }

    reportAvailableIds();

    LOG.debug("[{}] Writing {} on stream id {}", logPrefix, message.responseCallback, streamId);
    Frame frame =
        Frame.forRequest(
            protocolVersion.getCode(),
            streamId,
            message.tracing,
            message.customPayload,
            message.request);

    inFlight.put(streamId, message.responseCallback);
    ChannelFuture writeFuture = ctx.write(frame, promise);
    if (message.responseCallback.holdStreamId()) {
      writeFuture.addListener(
          future -> {
            if (future.isSuccess()) {
              message.responseCallback.onStreamIdAssigned(streamId);
            }
          });
    }
  }

  private void cancel(
      ChannelHandlerContext ctx, ResponseCallback responseCallback, ChannelPromise promise) {
    Integer streamId = inFlight.inverse().remove(responseCallback);
    if (streamId == null) {
      LOG.debug(
          "[{}] Received cancellation request for unknown callback {}, skipping",
          logPrefix,
          responseCallback);
    } else {
      LOG.debug(
          "[{}] Cancelled callback {} for stream id {}", logPrefix, responseCallback, streamId);
      if (closingGracefully && inFlight.isEmpty()) {
        LOG.debug("[{}] Last pending query was cancelled, closing channel", logPrefix);
        ctx.channel().close();
      } else {
        // We can't release the stream id, because a response might still come back from the server.
        // Keep track of how many of those ids are held, because we want to replace the channel if
        // it becomes too high.
        orphanStreamIds += 1;
        if (orphanStreamIds > maxOrphanStreamIds) {
          LOG.debug(
              "[{}] Orphan stream ids exceeded the configured threshold ({}), closing gracefully",
              logPrefix,
              maxOrphanStreamIds);
          startGracefulShutdown(ctx);
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
      LOG.debug("[{}] There are pending queries, delaying graceful shutdown", logPrefix);
      closingGracefully = true;
      closeStartedFuture.setSuccess();
    }
  }

  @Override
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
          LOG.warn("[{}] Unexpected error while invoking event handler", logPrefix, t);
        }
      }
    } else {
      ResponseCallback responseCallback = inFlight.get(streamId);
      if (responseCallback == null) {
        LOG.debug("[{}] Got response on orphan stream id {}, releasing", logPrefix, streamId);
        release(streamId, ctx);
        orphanStreamIds -= 1;
      } else {
        LOG.debug(
            "[{}] Got response on stream id {}, completing {}",
            logPrefix,
            streamId,
            responseCallback);
        if (!responseCallback.holdStreamId()) {
          release(streamId, ctx);
        }
        try {
          responseCallback.onResponse(responseFrame);
        } catch (Throwable t) {
          LOG.warn("[{}] Unexpected error while invoking response handler", logPrefix, t);
        }
      }
    }
  }

  /** Called if an exception was thrown while processing an inbound event (i.e. a response). */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    if (cause instanceof FrameDecodingException) {
      int streamId = ((FrameDecodingException) cause).streamId;
      LOG.debug("[{}] Error while decoding response on stream id {}", logPrefix, streamId);
      if (streamId >= 0) {
        // We know which request matches the failing response, fail that one only
        ResponseCallback responseCallback = release(streamId, ctx);
        try {
          responseCallback.onFailure(cause.getCause());
        } catch (Throwable t) {
          LOG.warn("[{}] Unexpected error while invoking failure handler", logPrefix, t);
        }
      } else {
        LOG.warn(
            "[{}] Unexpected error while decoding incoming event frame",
            logPrefix,
            cause.getCause());
      }
    } else {
      // Otherwise fail all pending requests
      abortAllInFlight(new ClosedConnectionException("Unexpected error on channel", cause));
      ctx.close();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
    if (event instanceof ReleaseEvent) {
      int streamId = ((ReleaseEvent) event).streamId;
      LOG.debug("[{}] Releasing stream id {}", logPrefix, streamId);
      release(streamId, ctx);
    } else if (event instanceof SetKeyspaceEvent) {
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

  private ResponseCallback release(int streamId, ChannelHandlerContext ctx) {
    LOG.debug("[{}] Releasing stream id {}", logPrefix, streamId);
    ResponseCallback responseCallback = inFlight.remove(streamId);
    streamIds.release(streamId);
    reportAvailableIds();
    // If we're in the middle of an orderly close and this was the last request, actually close
    // the channel now
    if (closingGracefully && inFlight.isEmpty()) {
      LOG.debug("[{}] Done handling the last pending query, closing channel", logPrefix);
      ctx.channel().close();
    }
    return responseCallback;
  }

  private void abortAllInFlight(ClosedConnectionException cause) {
    abortAllInFlight(cause, null);
  }

  /**
   * @param ignore the ResponseCallback that called this method, if applicable (avoids a recursive
   *     loop)
   */
  private void abortAllInFlight(ClosedConnectionException cause, ResponseCallback ignore) {
    for (ResponseCallback responseCallback : inFlight.values()) {
      if (responseCallback != ignore) {
        responseCallback.onFailure(cause);
      }
    }
    inFlight.clear();
    // It's not necessary to release the stream ids, since we always call this method right before
    // closing the channel
  }

  private void reportAvailableIds() {
    if (availableIdsHolder != null) {
      availableIdsHolder.value = streamIds.getAvailableIds();
    }
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
      return "[" + logPrefix + "] set keyspace " + keyspaceName;
    }

    @Override
    Message getRequest() {
      return new Query("USE " + keyspaceName.asCql());
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
        LOG.warn("[{}] Unexpected error while switching keyspace", logPrefix, setKeyspaceException);
        abortAllInFlight(setKeyspaceException, this);
        ctx.channel().close();
      }
    }
  }
}
