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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.ClusterNameMismatchException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.config.ConfigChangeEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The channel pool maintains a set of {@link DriverChannel} instances connected to a given node.
 *
 * <p>It allows clients to obtain a channel to execute their requests.
 *
 * <p>If one or more channels go down, a reconnection process starts in order to replace them; it
 * runs until the channel count is back to its intended target.
 */
@ThreadSafe
public class ChannelPool implements AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelPool.class);

  /**
   * Initializes a new pool.
   *
   * <p>The returned completion stage will complete when all the underlying channels have finished
   * their initialization. If one or more channels fail, a reconnection will be started immediately.
   * Note that this method succeeds even if all channels fail, so you might get a pool that has no
   * channels (i.e. {@link #next()} return {@code null}) and is reconnecting.
   */
  public static CompletionStage<ChannelPool> init(
      Node node,
      CqlIdentifier keyspaceName,
      NodeDistance distance,
      InternalDriverContext context,
      String sessionLogPrefix) {
    ChannelPool pool = new ChannelPool(node, keyspaceName, distance, context, sessionLogPrefix);
    return pool.connect();
  }

  // This is read concurrently, but only mutated on adminExecutor (by methods in SingleThreaded)
  @VisibleForTesting final ChannelSet channels = new ChannelSet();

  private final Node node;
  private final CqlIdentifier initialKeyspaceName;
  private final EventExecutor adminExecutor;
  private final String sessionLogPrefix;
  private final String logPrefix;
  private final SingleThreaded singleThreaded;
  private volatile boolean invalidKeyspace;

  private ChannelPool(
      Node node,
      CqlIdentifier keyspaceName,
      NodeDistance distance,
      InternalDriverContext context,
      String sessionLogPrefix) {
    this.node = node;
    this.initialKeyspaceName = keyspaceName;
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.sessionLogPrefix = sessionLogPrefix;
    this.logPrefix = sessionLogPrefix + "|" + node.getConnectAddress();
    this.singleThreaded = new SingleThreaded(keyspaceName, distance, context);
  }

  private CompletionStage<ChannelPool> connect() {
    RunOrSchedule.on(adminExecutor, singleThreaded::connect);
    return singleThreaded.connectFuture;
  }

  public Node getNode() {
    return node;
  }

  /**
   * The keyspace with which the pool was initialized (therefore a constant, it's not updated if the
   * keyspace is switched later).
   */
  public CqlIdentifier getInitialKeyspaceName() {
    return initialKeyspaceName;
  }

  /**
   * Whether all channels failed due to an invalid keyspace. This is only used at initialization. We
   * don't make the decision to close the pool here yet, that's done at the session level.
   */
  public boolean isInvalidKeyspace() {
    return invalidKeyspace;
  }

  /**
   * @return the channel that has the most available stream ids. This is called on the direct
   *     request path, and we want to avoid complex check-then-act semantics; therefore this might
   *     race and return a channel that is already closed, or {@code null}. In those cases, it is up
   *     to the caller to fail fast and move to the next node.
   *     <p>There is no need to return the channel.
   */
  public DriverChannel next() {
    return channels.next();
  }

  /** @return the number of active channels in the pool. */
  public int size() {
    return channels.size();
  }

  /** @return the number of available stream ids on all channels in the pool. */
  public int getAvailableIds() {
    return channels.getAvailableIds();
  }

  /**
   * @return the number of requests currently executing on all channels in this pool (including
   *     {@link #getOrphanedIds() orphaned ids}).
   */
  public int getInFlight() {
    return channels.getInFlight();
  }

  /**
   * @return the number of stream ids for requests in all channels in this pool that have either
   *     timed out or been cancelled, but for which we can't release the stream id because a request
   *     might still come from the server.
   */
  public int getOrphanedIds() {
    return channels.getOrphanedIds();
  }

  /**
   * Sets a new distance for the node this pool belongs to. This method returns immediately, the new
   * distance will be set asynchronously.
   *
   * @param newDistance the new distance to set.
   */
  public void resize(NodeDistance newDistance) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.resize(newDistance));
  }

  /**
   * Changes the keyspace name on all the channels in this pool.
   *
   * <p>Note that this is not called directly by the user, but happens only on a SetKeyspace
   * response after a successful "USE ..." query, so the name should be valid. If the keyspace
   * switch fails on any channel, that channel is closed and a reconnection is started.
   */
  public CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspaceName) {
    return RunOrSchedule.on(adminExecutor, () -> singleThreaded.setKeyspace(newKeyspaceName));
  }

  public void reconnectNow() {
    RunOrSchedule.on(adminExecutor, singleThreaded::reconnectNow);
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  /** Holds all administration tasks, that are confined to the admin executor. */
  private class SingleThreaded {

    private final DriverConfig config;
    private final ChannelFactory channelFactory;
    private final EventBus eventBus;
    // The channels that are currently connecting
    private final List<CompletionStage<DriverChannel>> pendingChannels = new ArrayList<>();
    private final Set<DriverChannel> closingChannels = new HashSet<>();
    private final Reconnection reconnection;
    private final Object configListenerKey;

    private NodeDistance distance;
    private int wantedCount;
    private final CompletableFuture<ChannelPool> connectFuture = new CompletableFuture<>();
    private boolean isConnecting;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean isClosing;
    private CompletableFuture<Void> setKeyspaceFuture;

    private CqlIdentifier keyspaceName;

    private SingleThreaded(
        CqlIdentifier keyspaceName, NodeDistance distance, InternalDriverContext context) {
      this.keyspaceName = keyspaceName;
      this.config = context.getConfig();
      this.distance = distance;
      this.wantedCount = getConfiguredSize(distance);
      this.channelFactory = context.getChannelFactory();
      this.eventBus = context.getEventBus();
      ReconnectionPolicy reconnectionPolicy = context.getReconnectionPolicy();
      this.reconnection =
          new Reconnection(
              logPrefix,
              adminExecutor,
              () -> reconnectionPolicy.newNodeSchedule(node),
              this::addMissingChannels,
              () -> eventBus.fire(ChannelEvent.reconnectionStarted(node)),
              () -> eventBus.fire(ChannelEvent.reconnectionStopped(node)));
      this.configListenerKey =
          eventBus.register(
              ConfigChangeEvent.class, RunOrSchedule.on(adminExecutor, this::onConfigChanged));
    }

    private void connect() {
      assert adminExecutor.inEventLoop();
      if (isConnecting) {
        return;
      }
      isConnecting = true;
      CompletionStage<ChannelPool> initialChannels =
          addMissingChannels()
              .thenApply(
                  allConnected -> {
                    if (!allConnected) {
                      reconnection.start();
                    }
                    return ChannelPool.this;
                  });
      CompletableFutures.completeFrom(initialChannels, connectFuture);
    }

    private CompletionStage<Boolean> addMissingChannels() {
      assert adminExecutor.inEventLoop();
      // We always wait for all attempts to succeed or fail before scheduling a reconnection
      assert pendingChannels.isEmpty();

      int missing = wantedCount - channels.size();
      LOG.debug("[{}] Trying to create {} missing channels", logPrefix, missing);
      DriverChannelOptions options =
          DriverChannelOptions.builder()
              .withKeyspace(keyspaceName)
              .withOwnerLogPrefix(sessionLogPrefix)
              .build();
      for (int i = 0; i < missing; i++) {
        CompletionStage<DriverChannel> channelFuture = channelFactory.connect(node, options);
        pendingChannels.add(channelFuture);
      }
      return CompletableFutures.allDone(pendingChannels)
          .thenApplyAsync(this::onAllConnected, adminExecutor);
    }

    private boolean onAllConnected(@SuppressWarnings("unused") Void v) {
      assert adminExecutor.inEventLoop();
      Throwable fatalError = null;
      int invalidKeyspaceErrors = 0;
      for (CompletionStage<DriverChannel> pendingChannel : pendingChannels) {
        CompletableFuture<DriverChannel> future = pendingChannel.toCompletableFuture();
        assert future.isDone();
        if (future.isCompletedExceptionally()) {
          Throwable error = CompletableFutures.getFailed(future);
          ((DefaultNode) node)
              .getMetricUpdater()
              .incrementCounter(
                  error instanceof AuthenticationException
                      ? DefaultNodeMetric.AUTHENTICATION_ERRORS
                      : DefaultNodeMetric.CONNECTION_INIT_ERRORS,
                  null);
          if (error instanceof ClusterNameMismatchException
              || error instanceof UnsupportedProtocolVersionException) {
            // This will likely be thrown by all channels, but finish the loop cleanly
            fatalError = error;
          } else if (error instanceof AuthenticationException) {
            // Always warn because this is most likely something the operator needs to fix.
            // Keep going to reconnect if it can be fixed without bouncing the client.
            Loggers.warnWithException(LOG, "[{}] Authentication error", logPrefix, error);
          } else if (error instanceof InvalidKeyspaceException) {
            invalidKeyspaceErrors += 1;
          } else {
            if (config
                .getDefaultProfile()
                .getBoolean(DefaultDriverOption.CONNECTION_WARN_INIT_ERROR)) {
              Loggers.warnWithException(
                  LOG, "[{}]  Error while opening new channel", logPrefix, error);
            } else {
              LOG.debug("[{}]  Error while opening new channel", logPrefix, error);
            }
          }
        } else {
          DriverChannel channel = CompletableFutures.getCompleted(future);
          if (isClosing) {
            LOG.debug(
                "[{}] New channel added ({}) but the pool was closed, closing it",
                logPrefix,
                channel);
            channel.forceClose();
          } else {
            LOG.debug("[{}] New channel added {}", logPrefix, channel);
            channels.add(channel);
            eventBus.fire(ChannelEvent.channelOpened(node));
            channel
                .closeStartedFuture()
                .addListener(
                    f ->
                        adminExecutor
                            .submit(() -> onChannelCloseStarted(channel))
                            .addListener(UncaughtExceptions::log));
            channel
                .closeFuture()
                .addListener(
                    f ->
                        adminExecutor
                            .submit(() -> onChannelClosed(channel))
                            .addListener(UncaughtExceptions::log));
          }
        }
      }
      // If all channels failed, assume the keyspace is wrong
      invalidKeyspace =
          invalidKeyspaceErrors > 0 && invalidKeyspaceErrors == pendingChannels.size();

      pendingChannels.clear();

      if (fatalError != null) {
        Loggers.warnWithException(
            LOG,
            "[{}] Fatal error while initializing pool, forcing the node down",
            logPrefix,
            fatalError);
        eventBus.fire(TopologyEvent.forceDown(node.getConnectAddress()));
        // Don't bother continuing, the pool will get shut down soon anyway
        return true;
      }

      shrinkIfTooManyChannels(); // Can happen if the pool was shrinked during the reconnection

      int currentCount = channels.size();
      LOG.debug(
          "[{}] Reconnection attempt complete, {}/{} channels",
          logPrefix,
          currentCount,
          wantedCount);
      // Stop reconnecting if we have the wanted count
      return currentCount >= wantedCount;
    }

    private void onChannelCloseStarted(DriverChannel channel) {
      assert adminExecutor.inEventLoop();
      if (!isClosing) {
        LOG.debug("[{}] Channel {} started graceful shutdown", logPrefix, channel);
        channels.remove(channel);
        closingChannels.add(channel);
        eventBus.fire(ChannelEvent.channelClosed(node));
        reconnection.start();
      }
    }

    private void onChannelClosed(DriverChannel channel) {
      assert adminExecutor.inEventLoop();
      if (!isClosing) {
        // Either it was closed abruptly and was still in the live set, or it was an orderly
        // shutdown and it had moved to the closing set.
        if (channels.remove(channel)) {
          LOG.debug("[{}] Lost channel {}", logPrefix, channel);
          eventBus.fire(ChannelEvent.channelClosed(node));
          reconnection.start();
        } else {
          LOG.debug("[{}] Channel {} completed graceful shutdown", logPrefix, channel);
          closingChannels.remove(channel);
        }
      }
    }

    private void resize(NodeDistance newDistance) {
      assert adminExecutor.inEventLoop();
      distance = newDistance;
      int newChannelCount = getConfiguredSize(newDistance);
      if (newChannelCount > wantedCount) {
        LOG.debug("[{}] Growing ({} => {} channels)", logPrefix, wantedCount, newChannelCount);
        wantedCount = newChannelCount;
        reconnection.start();
      } else if (newChannelCount < wantedCount) {
        LOG.debug("[{}] Shrinking ({} => {} channels)", logPrefix, wantedCount, newChannelCount);
        wantedCount = newChannelCount;
        if (!reconnection.isRunning()) {
          shrinkIfTooManyChannels();
        } // else it will be handled at the end of the reconnection attempt
      }
    }

    private void shrinkIfTooManyChannels() {
      assert adminExecutor.inEventLoop();
      int extraCount = channels.size() - wantedCount;
      if (extraCount > 0) {
        LOG.debug("[{}] Closing {} extra channels", logPrefix, extraCount);
        Set<DriverChannel> toRemove = Sets.newHashSetWithExpectedSize(extraCount);
        for (DriverChannel channel : channels) {
          toRemove.add(channel);
          if (--extraCount == 0) {
            break;
          }
        }
        for (DriverChannel channel : toRemove) {
          channels.remove(channel);
          channel.close();
          eventBus.fire(ChannelEvent.channelClosed(node));
        }
      }
    }

    private void onConfigChanged(@SuppressWarnings("unused") ConfigChangeEvent event) {
      assert adminExecutor.inEventLoop();
      // resize re-reads the pool size from the configuration and does nothing if it hasn't changed,
      // which is exactly what we want.
      resize(distance);
    }

    private CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspaceName) {
      assert adminExecutor.inEventLoop();
      if (setKeyspaceFuture != null && !setKeyspaceFuture.isDone()) {
        return CompletableFutures.failedFuture(
            new IllegalStateException(
                "Can't call setKeyspace while a keyspace switch is already in progress"));
      }
      keyspaceName = newKeyspaceName;
      setKeyspaceFuture = new CompletableFuture<>();

      // Switch the keyspace on all live channels.
      // We can read the size before iterating because mutations are confined to this thread:
      int toSwitch = channels.size();
      if (toSwitch == 0) {
        setKeyspaceFuture.complete(null);
      } else {
        AtomicInteger remaining = new AtomicInteger(toSwitch);
        for (DriverChannel channel : channels) {
          channel
              .setKeyspace(newKeyspaceName)
              .addListener(
                  f -> {
                    // Don't handle errors: if a channel fails to switch the keyspace, it closes
                    if (remaining.decrementAndGet() == 0) {
                      setKeyspaceFuture.complete(null);
                    }
                  });
        }
      }

      // pending channels were scheduled with the old keyspace name, ensure they eventually switch
      for (CompletionStage<DriverChannel> channelFuture : pendingChannels) {
        // errors are swallowed here, this is fine because a setkeyspace error will close the
        // channel, so it will eventually get reported
        channelFuture.thenAccept(channel -> channel.setKeyspace(newKeyspaceName));
      }

      return setKeyspaceFuture;
    }

    private void reconnectNow() {
      assert adminExecutor.inEventLoop();
      // Don't force because if the reconnection is stopped, it means either we have enough channels
      // or the pool is shutting down.
      reconnection.reconnectNow(false);
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (isClosing) {
        return;
      }
      isClosing = true;

      // If an attempt was in progress right now, it might open new channels but they will be
      // handled in onAllConnected
      reconnection.stop();

      eventBus.unregister(configListenerKey, ConfigChangeEvent.class);

      // Close all channels, the pool future completes when all the channels futures have completed
      int toClose = closingChannels.size() + channels.size();
      if (toClose == 0) {
        closeFuture.complete(null);
      } else {
        AtomicInteger remaining = new AtomicInteger(toClose);
        GenericFutureListener<Future<? super Void>> channelCloseListener =
            f -> {
              if (!f.isSuccess()) {
                Loggers.warnWithException(LOG, "[{}] Error closing channel", logPrefix, f.cause());
              }
              if (remaining.decrementAndGet() == 0) {
                closeFuture.complete(null);
              }
            };
        for (DriverChannel channel : channels) {
          eventBus.fire(ChannelEvent.channelClosed(node));
          channel.close().addListener(channelCloseListener);
        }
        for (DriverChannel channel : closingChannels) {
          // don't fire the close event, onChannelCloseStarted() already did it
          channel.closeFuture().addListener(channelCloseListener);
        }
      }
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (!isClosing) {
        close();
      }
      for (DriverChannel channel : channels) {
        channel.forceClose();
      }
      for (DriverChannel channel : closingChannels) {
        channel.forceClose();
      }
    }

    private int getConfiguredSize(NodeDistance distance) {
      return config
          .getDefaultProfile()
          .getInt(
              (distance == NodeDistance.LOCAL)
                  ? DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE
                  : DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE);
    }
  }
}
