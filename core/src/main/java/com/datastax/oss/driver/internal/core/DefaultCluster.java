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

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateManager;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
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

  private class SingleThreaded {

    private final InternalDriverContext context;
    private final Set<InetSocketAddress> initialContactPoints;
    private final CompletableFuture<Cluster> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;

    private SingleThreaded(InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
      this.context = context;
      this.initialContactPoints = contactPoints;
    }

    private void init() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;

      new NodeStateManager(context);

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
                metadataManager.refreshNodes();
                // TODO schedule full schema refresh
              })
          .exceptionally(
              error -> {
                initFuture.completeExceptionally(error);
                return null;
              });
    }
  }
}
