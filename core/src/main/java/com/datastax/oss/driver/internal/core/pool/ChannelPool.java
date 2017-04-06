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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Function;
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
public class ChannelPool {
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
      SocketAddress address,
      CqlIdentifier keyspaceName,
      int channelCount,
      InternalDriverContext context) {
    ChannelPool pool = new ChannelPool(address, keyspaceName, channelCount, context);
    return pool.connect();
  }

  // This is read concurrently, but only mutated on adminExecutor (by methods in SingleThreaded)
  @VisibleForTesting final ChannelSet channels = new ChannelSet();

  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;

  private ChannelPool(
      SocketAddress address,
      CqlIdentifier keyspaceName,
      int channelCount,
      InternalDriverContext context) {

    adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(address, keyspaceName, channelCount, context);
  }

  private CompletionStage<ChannelPool> connect() {
    RunOrSchedule.on(adminExecutor, singleThreaded::connect);
    return singleThreaded.connectFuture;
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

  /**
   * Changes the keyspace name on all the channels in this pool.
   *
   * <p>Note that this is not called directly by the user, but happens only on a SetKeypsace
   * response after a successful "USE ..." query, so the name should be valid. If the keyspace
   * switch fails on any channel, that channel is closed and a reconnection is started.
   */
  public CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspaceName) {
    return RunOrSchedule.on(adminExecutor, () -> singleThreaded.setKeyspace(newKeyspaceName));
  }

  /**
   * Closes the pool gracefully: subsequent calls to {@link #next()} will fail, but the pool's
   * channels will be closed gracefully (allowing pending requests to complete).
   */
  public CompletionStage<ChannelPool> close() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  /**
   * Closes the pool forcefully: subsequent calls to {@link #next()} will fail, and the pool's
   * channels will be closed forcefully (aborting pending requests).
   */
  public CompletionStage<ChannelPool> forceClose() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  /** Does not close the pool, but returns a completion stage that will complete when it does. */
  public CompletionStage<ChannelPool> closeFuture() {
    return singleThreaded.closeFuture;
  }

  /** Holds all administration tasks, that are confined to the admin executor. */
  private class SingleThreaded {

    private final SocketAddress address;
    private final int wantedCount;
    private final ChannelFactory channelFactory;
    // The channels that are currently connecting
    private final List<CompletionStage<DriverChannel>> pendingChannels = new ArrayList<>();
    private final Reconnection reconnection;

    private CompletableFuture<ChannelPool> connectFuture = new CompletableFuture<>();
    private boolean isConnecting;
    private CompletableFuture<ChannelPool> closeFuture = new CompletableFuture<>();
    private boolean isClosing;
    private CompletableFuture<Void> setKeyspaceFuture;

    private CqlIdentifier keyspaceName;

    private SingleThreaded(
        SocketAddress address,
        CqlIdentifier keyspaceName,
        int wantedCount,
        InternalDriverContext context) {
      this.address = address;
      this.keyspaceName = keyspaceName;
      this.wantedCount = wantedCount;
      this.channelFactory = context.channelFactory();
      this.reconnection =
          new Reconnection(adminExecutor, context.reconnectionPolicy(), this::addMissingChannels);
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
      LOG.debug("{} trying to create {} missing channels", ChannelPool.this, missing);
      for (int i = 0; i < missing; i++) {
        CompletionStage<DriverChannel> channelFuture =
            channelFactory.connect(address, keyspaceName, wantedCount > 1);
        pendingChannels.add(channelFuture);
      }
      return CompletableFutures.whenAllDone(pendingChannels)
          .thenApplyAsync(this::onAllConnected, adminExecutor);
    }

    private boolean onAllConnected(@SuppressWarnings("unused") Void v) {
      assert adminExecutor.inEventLoop();
      for (CompletionStage<DriverChannel> pendingChannel : pendingChannels) {
        CompletableFuture<DriverChannel> future = pendingChannel.toCompletableFuture();
        assert future.isDone();
        try {
          DriverChannel channel = future.get();
          if (isClosing) {
            LOG.debug(
                "{} new channel added ({}) but the pool was closed, closing it",
                ChannelPool.this,
                channel);
            channel.forceClose();
          } else {
            LOG.debug("{} new channel added {}", ChannelPool.this, channel);
            channels.add(channel);
            channel
                .closeFuture()
                .addListener(
                    f ->
                        adminExecutor
                            .submit(() -> onChannelClosed(channel))
                            .addListener(UncaughtExceptions::log));
          }
        } catch (InterruptedException e) {
          // can't happen, the future is done
        } catch (ExecutionException e) {
          // TODO handle ClusterNameMismatchException
          LOG.debug(ChannelPool.this + " error while opening new channel", e.getCause());
          // TODO we don't log at a higher level because it's not a fatal error, but this should probably be recorded somewhere (metric?)
        }
      }
      pendingChannels.clear();

      int currentCount = channels.size();
      LOG.debug(
          "{} reconnection attempt complete, {}/{} channels",
          ChannelPool.this,
          currentCount,
          wantedCount);
      // Stop reconnecting if we have the wanted count
      return currentCount >= wantedCount;
    }

    private void onChannelClosed(DriverChannel channel) {
      assert adminExecutor.inEventLoop();
      LOG.debug("{} lost channel {}", ChannelPool.this, channel);
      channels.remove(channel);
      if (!isClosing && !reconnection.isRunning()) {
        reconnection.start();
      }
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
      // Note that we don't handle errors; if the keyspace switch fails, the channel closes
      forAllChannels(
          ch -> ch.setKeyspace(newKeyspaceName), () -> setKeyspaceFuture.complete(null), null);

      // pending channels were scheduled with the old keyspace name, ensure they eventually switch
      for (CompletionStage<DriverChannel> channelFuture : pendingChannels) {
        // errors are swallowed here, this is fine because a setkeyspace error will close the
        // channel, so it will eventually get reported
        channelFuture.thenAccept(channel -> channel.setKeyspace(newKeyspaceName));
      }

      return setKeyspaceFuture;
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (isClosing) {
        return;
      }
      isClosing = true;

      reconnection.stop();

      forAllChannels(
          DriverChannel::close,
          () -> closeFuture.complete(ChannelPool.this),
          (channel, error) ->
              LOG.warn(ChannelPool.this + " error closing channel " + channel, error));
    }

    private <V> void forAllChannels(
        Function<DriverChannel, Future<V>> task,
        Runnable whenAllDone,
        BiConsumer<DriverChannel, Throwable> onError) {
      assert adminExecutor.inEventLoop();
      // we can read the size before iterating because it's only mutated from this thread
      int todo = channels.size();
      if (todo == 0) {
        whenAllDone.run();
      } else {
        AtomicInteger done = new AtomicInteger();
        for (DriverChannel channel : channels) {
          task.apply(channel)
              .addListener(
                  f -> {
                    if (!f.isSuccess() && onError != null) {
                      onError.accept(channel, f.cause());
                    } else if (done.incrementAndGet() == todo) {
                      whenAllDone.run();
                    }
                  });
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
    }
  }
}
