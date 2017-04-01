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
import com.datastax.oss.driver.api.core.connection.ConnectionException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.ReleaseEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.RequestMessage;
import com.datastax.oss.driver.internal.core.channel.DriverChannel.SetKeyspaceEvent;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Promise;
import java.util.Map;

/** Manages requests that are currently executing on a channel. */
public class InFlightHandler extends ChannelDuplexHandler {
  private final ProtocolVersion protocolVersion;
  private final StreamIdGenerator streamIds;
  private final Map<Integer, ResponseCallback> inFlight;
  private final long setKeyspaceTimeoutMillis;
  private ChannelPromise closePromise;
  private SetKeyspaceRequest setKeyspaceRequest;

  InFlightHandler(
      ProtocolVersion protocolVersion, StreamIdGenerator streamIds, long setKeyspaceTimeoutMillis) {
    this.protocolVersion = protocolVersion;
    this.streamIds = streamIds;
    this.inFlight = Maps.newHashMapWithExpectedSize(streamIds.getMaxAvailableIds());
    this.setKeyspaceTimeoutMillis = setKeyspaceTimeoutMillis;
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (closePromise != null) {
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

    RequestMessage message = (RequestMessage) msg;
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

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    Frame responseFrame = (Frame) msg;
    int streamId = responseFrame.streamId;

    ResponseCallback responseCallback = inFlight.get(streamId);
    if (responseCallback != null) {
      if (!responseCallback.holdStreamId()) {
        release(streamId, ctx);
      }
      responseCallback.onResponse(responseFrame);
    }
    super.channelRead(ctx, msg);
  }

  /** Called if an exception was thrown while processing an inbound event (i.e. a response). */
  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    int streamId;
    if (cause instanceof FrameDecodingException
        && (streamId = ((FrameDecodingException) cause).streamId) >= 0) {
      // We know which request matches the failing response, fail that one only
      ResponseCallback responseCallback = release(streamId, ctx);
      responseCallback.onFailure(cause.getCause());
    } else {
      // Otherwise fail all pending requests
      abortAllInFlight(new ConnectionException("Unexpected error on channel", cause));
      ctx.close();
    }
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
    // Delay the actual close if there are pending requests
    if (inFlight.isEmpty()) {
      super.close(ctx, promise);
    } else {
      this.closePromise = promise;
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object event) throws Exception {
    if (event == DriverChannel.FORCE_CLOSE_EVENT) {
      Preconditions.checkState(
          closePromise != null, "Channel should be closed before sending FORCE_CLOSE event");
      // Note: this is guaranteed by DriverChannel.forceClose

      abortAllInFlight(new ConnectionException("Channel was force-closed"));
      super.close(ctx, closePromise);
    } else if (event instanceof ReleaseEvent) {
      release(((ReleaseEvent) event).streamId, ctx);
    } else if (event instanceof SetKeyspaceEvent) {
      SetKeyspaceEvent setKeyspaceEvent = (SetKeyspaceEvent) event;
      if (this.setKeyspaceRequest != null) {
        setKeyspaceEvent.promise.setFailure(
            new IllegalStateException(
                "Got a keyspace change request while another one was already in progress. "
                    + "This is generally a sign that your application issues USE queries too rapidly."));
      } else {
        this.setKeyspaceRequest = new SetKeyspaceRequest(ctx, setKeyspaceEvent);
        this.setKeyspaceRequest.send();
      }
    } else {
      super.userEventTriggered(ctx, event);
    }
  }

  private ResponseCallback release(int streamId, ChannelHandlerContext ctx) {
    ResponseCallback responseCallback = inFlight.remove(streamId);
    streamIds.release(streamId);
    // If we're in the middle of an orderly close and this was the last request, actually close
    // the channel now
    if (closePromise != null && inFlight.isEmpty()) {
      try {
        super.close(ctx, closePromise);
      } catch (Exception e) {
        ctx.fireExceptionCaught(e);
      }
    }
    return responseCallback;
  }

  private void abortAllInFlight(Throwable cause) {
    for (ResponseCallback responseCallback : inFlight.values()) {
      responseCallback.onFailure(cause);
    }
    inFlight.clear();
    // It's not necessary to release the stream ids, since we always call this method right before
    // closing the channel
  }

  private class SetKeyspaceRequest extends InternalRequest {

    private final CqlIdentifier keyspaceName;
    private final Promise<Void> promise;

    SetKeyspaceRequest(ChannelHandlerContext ctx, SetKeyspaceEvent setKeyspaceEvent) {
      super(ctx, setKeyspaceTimeoutMillis);
      this.keyspaceName = setKeyspaceEvent.keyspaceName;
      this.promise = setKeyspaceEvent.promise;
    }

    @Override
    String describe() {
      return "set keyspace " + keyspaceName;
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
    void fail(Throwable cause) {
      if (promise.tryFailure(cause)) {
        InFlightHandler.this.setKeyspaceRequest = null;
      }
    }
  }
}
