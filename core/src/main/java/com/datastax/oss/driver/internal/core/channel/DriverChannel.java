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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.protocol.internal.Message;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoop;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;

/**
 * A thin wrapper around a Netty {@link Channel}, to send requests to a Cassandra node and receive
 * responses.
 */
@ThreadSafe
public class DriverChannel {

  static final AttributeKey<String> CLUSTER_NAME_KEY = AttributeKey.newInstance("cluster_name");
  static final AttributeKey<Map<String, List<String>>> OPTIONS_KEY =
      AttributeKey.newInstance("options");

  @SuppressWarnings("RedundantStringConstructorCall")
  static final Object GRACEFUL_CLOSE_MESSAGE = new String("GRACEFUL_CLOSE_MESSAGE");

  @SuppressWarnings("RedundantStringConstructorCall")
  static final Object FORCEFUL_CLOSE_MESSAGE = new String("FORCEFUL_CLOSE_MESSAGE");

  private final EndPoint endPoint;
  private final Channel channel;
  private final InFlightHandler inFlightHandler;
  private final WriteCoalescer writeCoalescer;
  private final ProtocolVersion protocolVersion;
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicBoolean forceClosing = new AtomicBoolean();

  DriverChannel(
      EndPoint endPoint,
      Channel channel,
      WriteCoalescer writeCoalescer,
      ProtocolVersion protocolVersion) {
    this.endPoint = endPoint;
    this.channel = channel;
    this.inFlightHandler = channel.pipeline().get(InFlightHandler.class);
    this.writeCoalescer = writeCoalescer;
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
      return channel.newFailedFuture(new IllegalStateException("Driver channel is closing"));
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
    // To avoid creating an extra message, we adopt the convention that writing the callback
    // directly means cancellation
    writeCoalescer.writeAndFlush(channel, responseCallback).addListener(UncaughtExceptions::log);
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

  public Map<String, List<String>> getOptions() {
    return channel.attr(OPTIONS_KEY).get();
  }

  /**
   * @return the number of available stream ids on the channel; more precisely, this is the number
   *     of {@link #preAcquireId()} calls for which the id has not been released yet. This is used
   *     to weigh channels in pools that have a size bigger than 1, in the load balancing policy,
   *     and for monitoring purposes.
   */
  public int getAvailableIds() {
    return inFlightHandler.getAvailableIds();
  }

  /**
   * Indicates the intention to send a request using this channel.
   *
   * <p>There must be <b>exactly one</b> invocation of this method before each call to {@link
   * #write(Message, boolean, Map, ResponseCallback)}. If this method returns true, the client
   * <b>must</b> proceed with the write. If it returns false, it <b>must not</b> proceed.
   *
   * <p>This method is used together with {@link #getAvailableIds()} to track how many requests are
   * currently executing on the channel, and avoid submitting a request that would result in a
   * {@link BusyConnectionException}. The two methods follow atomic semantics: {@link
   * #getAvailableIds()} returns the exact count of clients that have called {@link #preAcquireId()}
   * and not yet released their stream id at this point in time.
   *
   * <p>Most of the time, the driver code calls this method automatically:
   *
   * <ul>
   *   <li>if you obtained the channel from a pool ({@link ChannelPool#next()} or {@link
   *       DefaultSession#getChannel(Node, String)}), <b>do not call</b> this method: it has already
   *       been done as part of selecting the channel.
   *   <li>if you use {@link ChannelHandlerRequest} or {@link AdminRequestHandler} for internal
   *       queries, <b>do not call</b> this method, those classes already do it.
   *   <li>however, if you use {@link ThrottledAdminRequestHandler}, you must specify a {@code
   *       shouldPreAcquireId} argument to indicate whether to call this method or not. This is
   *       because those requests are sometimes used with a channel that comes from a pool
   *       (requiring {@code shouldPreAcquireId = false}), or sometimes with a standalone channel
   *       like in the control connection (requiring {@code shouldPreAcquireId = true}).
   * </ul>
   */
  public boolean preAcquireId() {
    return inFlightHandler.preAcquireId();
  }

  /**
   * @return the number of requests currently executing on this channel (including {@link
   *     #getOrphanedIds() orphaned ids}).
   */
  public int getInFlight() {
    return inFlightHandler.getInFlight();
  }

  /**
   * @return the number of stream ids for requests that have either timed out or been cancelled, but
   *     for which we can't release the stream id because a request might still come from the
   *     server.
   */
  public int getOrphanedIds() {
    return inFlightHandler.getOrphanIds();
  }

  public EventLoop eventLoop() {
    return channel.eventLoop();
  }

  public ProtocolVersion protocolVersion() {
    return protocolVersion;
  }

  /** The endpoint that was used to establish the connection. */
  public EndPoint getEndPoint() {
    return endPoint;
  }

  public SocketAddress localAddress() {
    return channel.localAddress();
  }

  /** @return The {@link ChannelConfig configuration} of this channel. */
  public ChannelConfig config() {
    return channel.config();
  }

  /**
   * Initiates a graceful shutdown: no new requests will be accepted, but all pending requests will
   * be allowed to complete before the underlying channel is closed.
   */
  public Future<Void> close() {
    if (closing.compareAndSet(false, true) && channel.isOpen()) {
      // go through the coalescer: this guarantees that we won't reject writes that were submitted
      // before, but had not been coalesced yet.
      writeCoalescer
          .writeAndFlush(channel, GRACEFUL_CLOSE_MESSAGE)
          .addListener(UncaughtExceptions::log);
    }
    return channel.closeFuture();
  }

  /**
   * Initiates a forced shutdown: any pending request will be aborted and the underlying channel
   * will be closed.
   */
  public Future<Void> forceClose() {
    this.close();
    if (forceClosing.compareAndSet(false, true) && channel.isOpen()) {
      writeCoalescer
          .writeAndFlush(channel, FORCEFUL_CLOSE_MESSAGE)
          .addListener(UncaughtExceptions::log);
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
    return this.inFlightHandler.closeStartedFuture;
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

  static class SetKeyspaceEvent {
    final CqlIdentifier keyspaceName;
    final Promise<Void> promise;

    public SetKeyspaceEvent(CqlIdentifier keyspaceName, Promise<Void> promise) {
      this.keyspaceName = keyspaceName;
      this.promise = promise;
    }
  }
}
