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
import com.datastax.oss.protocol.internal.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A thin wrapper around a Netty {@link Channel}, to send requests to a Cassandra node and receive
 * responses.
 */
public class DriverChannel {
  static final AttributeKey<String> CLUSTER_NAME_KEY = AttributeKey.newInstance("cluster_name");
  static final Object FORCE_CLOSE_EVENT = new Object();

  private final Channel channel;

  DriverChannel(Channel channel) {
    this.channel = channel;
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
    RequestMessage message = new RequestMessage(request, tracing, customPayload, responseCallback);
    return channel.writeAndFlush(message); //TODO coalesce flushes
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

  public Future<Void> close() {
    return channel.close();
  }

  public Future<Void> forceClose() {
    ChannelFuture closeFuture = channel.close();
    channel.pipeline().fireUserEventTriggered(FORCE_CLOSE_EVENT);
    return closeFuture;
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
