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
import com.datastax.oss.protocol.internal.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A thin wrapper around a Netty {@link Channel}, to send requests to a Cassandra node and receive
 * responses.
 */
public class DriverChannel {
  static final AttributeKey<String> CLUSTER_NAME_KEY = AttributeKey.newInstance("cluster_name");

  @SuppressWarnings("RedundantStringConstructorCall")
  static final Object GRACEFUL_CLOSE_MESSAGE = new String("GRACEFUL_CLOSE_MESSAGE");

  @SuppressWarnings("RedundantStringConstructorCall")
  static final Object FORCEFUL_CLOSE_MESSAGE = new String("FORCEFUL_CLOSE_MESSAGE");

  private final Channel channel;
  private final ChannelFuture closeStartedFuture;
  private final WriteCoalescer writeCoalescer;
  private final AvailableIdsHolder availableIdsHolder;
  private final ProtocolVersion protocolVersion;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean forceClosing = new AtomicBoolean();

  DriverChannel(
      Channel channel,
      WriteCoalescer writeCoalescer,
      AvailableIdsHolder availableIdsHolder,
      ProtocolVersion protocolVersion) {
    this.channel = channel;
    this.closeStartedFuture = channel.pipeline().get(InFlightHandler.class).closeStartedFuture;
    this.writeCoalescer = writeCoalescer;
    this.availableIdsHolder = availableIdsHolder;
    this.protocolVersion = protocolVersion;
  }

  /**
   * @return a future that succeeds when the request frame was successfully written on the channel.
   *     Beyond that, the caller will be notified through the {@code responseCallback}.
   */
  public Future<Void> write(
      Message request,
      boolean tracing,
      Map<String, ByteBuffer> customPayload,
      ResponseCallback responseCallback) {
    if (closing.get()) {
      throw new IllegalStateException("Driver channel is closing");
    }
    RequestMessage message = new RequestMessage(request, tracing, customPayload, responseCallback);
    return writeCoalescer.writeAndFlush(channel, message);
  }

  /**
   * Cancels a callback, indicating that the client that wrote it is no longer interested in the
   * answer.
   *
   * <p>Note that this does not cancel the request server-side (but might in the future if Cassandra
   * supports it).
   */
  public void cancel(ResponseCallback responseCallback) {
    if (closing.get()) {
      throw new IllegalStateException("Driver channel is closing");
    }
    // To avoid creating an extra message, we adopt the convention that writing the callback
    // directly means cancellation
    writeCoalescer.writeAndFlush(channel, responseCallback);
  }

  /**
   * Releases a stream id if the client was holding onto it, and has now determined that it can be
   * safely reused.
   *
   * @see ResponseCallback#holdStreamId()
   */
  public void release(int streamId) {
    channel.pipeline().fireUserEventTriggered(new ReleaseEvent(streamId));
  }

  /**
   * Switches the underlying Cassandra connection to a new keyspace (as if a {@code USE ...}
   * statement was issued).
   *
   * <p>The future will complete once the change is effective. Only one change may run at a given
   * time, concurrent attempts will fail.
   *
   * <p>Changing the keyspace is inherently thread-unsafe: if other queries are running at the same
   * time, the keyspace they will use is unpredictable.
   */
  public Future<Void> setKeyspace(CqlIdentifier newKeyspace) {
    Promise<Void> promise = channel.eventLoop().newPromise();
    channel.pipeline().fireUserEventTriggered(new SetKeyspaceEvent(newKeyspace, promise));
    return promise;
  }

  /**
   * @return the name of the Cassandra cluster as returned by {@code system.local.cluster_name} on
   *     this connection.
   */
  public String getClusterName() {
    return channel.attr(CLUSTER_NAME_KEY).get();
  }

  /**
   * @return the number of available stream ids on the channel. This is used to weigh channels in
   *     the pool. Note that for performance reasons this is only maintained if the channel is part
   *     of a pool that has a size bigger than 1, otherwise it will always return -1.
   */
  public int availableIds() {
    return (availableIdsHolder == null) ? -1 : availableIdsHolder.value;
  }

  public EventLoop eventLoop() {
    return channel.eventLoop();
  }

  public ProtocolVersion protocolVersion() {
    return protocolVersion;
  }

  public SocketAddress address() {
    return channel.remoteAddress();
  }

  /**
   * Initiates a graceful shutdown: no new requests will be accepted, but all pending requests will
   * be allowed to complete before the underlying channel is closed.
   */
  public Future<Void> close() {
    if (closing.compareAndSet(false, true)) {
      // go through the coalescer: this guarantees that we won't reject writes that were submitted
      // before, but had not been coalesced yet.
      writeCoalescer.writeAndFlush(channel, GRACEFUL_CLOSE_MESSAGE);
    }
    return channel.closeFuture();
  }

  /**
   * Initiates a forced shutdown: any pending request will be aborted and the underlying channel
   * will be closed.
   */
  public Future<Void> forceClose() {
    this.close();
    if (forceClosing.compareAndSet(false, true)) {
      writeCoalescer.writeAndFlush(channel, FORCEFUL_CLOSE_MESSAGE);
    }
    return channel.closeFuture();
  }

  /**
   * Returns a future that will complete when a graceful close has started, but not yet completed.
   *
   * <p>In other words, the channel has stopped accepting new requests, but is still waiting for
   * pending requests to finish. Once the last response has been received, the channel will really
   * close and {@link #closeFuture()} will be completed.
   *
   * <p>If there were no pending requests when the graceful shutdown was initiated, or if {@link
   * #forceClose()} is called first, this future will never complete.
   */
  public ChannelFuture closeStartedFuture() {
    return this.closeStartedFuture;
  }

  /**
   * Does not close the channel, but returns a future that will complete when it is completely
   * closed.
   */
  public ChannelFuture closeFuture() {
    return channel.closeFuture();
  }

  @Override
  public String toString() {
    return channel.toString();
  }

  // This is essentially a stripped-down Frame. We can't materialize the frame before writing,
  // because we need the stream id, which is assigned from within the event loop.
  static class RequestMessage {
    final Message request;
    final boolean tracing;
    final Map<String, ByteBuffer> customPayload;
    final ResponseCallback responseCallback;

    RequestMessage(
        Message message,
        boolean tracing,
        Map<String, ByteBuffer> customPayload,
        ResponseCallback responseCallback) {
      this.request = message;
      this.tracing = tracing;
      this.customPayload = customPayload;
      this.responseCallback = responseCallback;
    }
  }

  static class ReleaseEvent {
    final int streamId;

    ReleaseEvent(int streamId) {
      this.streamId = streamId;
    }
  }

  static class SetKeyspaceEvent {
    final CqlIdentifier keyspaceName;
    final Promise<Void> promise;

    public SetKeyspaceEvent(CqlIdentifier keyspaceName, Promise<Void> promise) {
      this.keyspaceName = keyspaceName;
      this.promise = promise;
    }
  }
}
