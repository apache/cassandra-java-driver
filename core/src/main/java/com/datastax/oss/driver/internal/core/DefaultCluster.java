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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateManager;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.google.common.collect.ImmutableList;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCluster implements Cluster {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCluster.class);

  public static CompletableFuture<Cluster> init(
      InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
    DefaultCluster cluster = new DefaultCluster(context, contactPoints);
    return cluster.init();
  }

  private final InternalDriverContext context;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  private final MetadataManager metadataManager;

  private DefaultCluster(InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
    this.context = context;
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context, contactPoints);
    this.metadataManager = context.metadataManager();
  }

  private CompletableFuture<Cluster> init() {
    RunOrSchedule.on(adminExecutor, singleThreaded::init);
    return singleThreaded.initFuture;
  }

  @Override
  public Metadata getMetadata() {
    return metadataManager.getMetadata();
  }

  @Override
  public DriverContext getContext() {
    return context;
  }

  @Override
  public CompletionStage<CqlSession> connectAsync(CqlIdentifier keyspace) {
    CompletableFuture<CqlSession> connectFuture = new CompletableFuture<>();
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.connect(keyspace, connectFuture));
    return connectFuture;
  }

  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  private class SingleThreaded {

    private final InternalDriverContext context;
    private final Set<InetSocketAddress> initialContactPoints;
    private final NodeStateManager nodeStateManager;
    private final CompletableFuture<Cluster> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private boolean forceCloseWasCalled;
    // Note: closed sessions are not removed from the list. If this creates a memory issue, there
    // is something really wrong in the client program
    private List<Session> sessions;

    private SingleThreaded(InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
      this.context = context;
      this.nodeStateManager = new NodeStateManager(context);
      this.initialContactPoints = contactPoints;
      this.sessions = new ArrayList<>();
    }

    private void init() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;

      // If any contact points were provided, store them in the metadata right away (the
      // control connection will need them if it has to initialize)
      MetadataManager metadataManager = context.metadataManager();
      metadataManager
          .addContactPoints(initialContactPoints)
          // Then initialize the topology monitor
          .thenCompose(v -> context.topologyMonitor().init())
          // If that succeeds, Cluster init is considered successful
          .thenAccept(
              v -> {
                initFuture.complete(DefaultCluster.this);

                // Launch a full refresh asynchronously
                metadataManager
                    .refreshNodes()
                    .whenComplete(
                        (result, error) -> {
                          if (error != null) {
                            LOG.debug("Error while refreshing node list", error);
                          } else {
                            try {
                              context.loadBalancingPolicyWrapper().init();
                            } catch (Throwable t) {
                              LOG.warn(
                                  "Unexpected error while initializing load balancing policy", t);
                            }
                          }
                        });

                // TODO schedule full schema refresh
              })
          .exceptionally(
              error -> {
                initFuture.completeExceptionally(error);
                return null;
              });
    }

    private void connect(CqlIdentifier keyspace, CompletableFuture<CqlSession> connectFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        connectFuture.completeExceptionally(new DriverException("Cluster was closed"));
      } else {
        DefaultSession.init(context, keyspace)
            .whenCompleteAsync(
                (session, error) -> {
                  if (error != null) {
                    connectFuture.completeExceptionally(error);
                  } else if (closeWasCalled) {
                    connectFuture.completeExceptionally(
                        new DriverException("Cluster was closed while session was initializing"));
                    session.forceCloseAsync();
                  } else {
                    sessions.add(session);
                    connectFuture.complete(session);
                  }
                },
                adminExecutor)
            .exceptionally(UncaughtExceptions::log);
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;

      LOG.debug("Closing {}", this);
      List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
      for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
        LOG.debug("Closing {}", closeable);
        childrenCloseStages.add(closeable.closeAsync());
      }
      CompletableFutures.whenAllDone(
          childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (forceCloseWasCalled) {
        return;
      }
      forceCloseWasCalled = true;

      LOG.debug("Force-closing {} (was {}closed before)", this, (closeWasCalled ? "" : "not "));

      if (closeWasCalled) {
        // onChildrenClosed has already been called
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          LOG.debug("Force-closing {}", closeable);
          closeable.forceCloseAsync();
        }
      } else {
        closeWasCalled = true;
        List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          LOG.debug("Force-closing {}", closeable);
          childrenCloseStages.add(closeable.forceCloseAsync());
        }
        CompletableFutures.whenAllDone(
            childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
      }
    }

    private void onChildrenClosed(List<CompletionStage<Void>> childrenCloseStages) {
      assert adminExecutor.inEventLoop();
      for (CompletionStage<Void> stage : childrenCloseStages) {
        warnIfFailed(stage);
      }
      context
          .nettyOptions()
          .onClose()
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  closeFuture.completeExceptionally(f.cause());
                } else {
                  closeFuture.complete(null);
                }
              });
    }

    private void warnIfFailed(CompletionStage<Void> stage) {
      CompletableFuture<Void> future = stage.toCompletableFuture();
      assert future.isDone();
      if (future.isCompletedExceptionally()) {
        LOG.warn("Unexpected error while closing", CompletableFutures.getFailed(future));
      }
    }

    private List<AsyncAutoCloseable> internalComponentsToClose() {
      return ImmutableList.<AsyncAutoCloseable>builder()
          .addAll(sessions)
          .add(
              nodeStateManager,
              metadataManager,
              context.topologyMonitor(),
              context.controlConnection())
          .build();
    }
  }
}
