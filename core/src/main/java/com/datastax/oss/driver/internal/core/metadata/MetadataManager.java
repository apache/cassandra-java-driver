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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Holds the immutable instance of the {@link Metadata}, and handles requests to update it. */
public class MetadataManager {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataManager.class);

  private final InternalDriverContext context;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  private volatile DefaultMetadata metadata; // must be updated on adminExecutor only

  public MetadataManager(InternalDriverContext context) {
    this.context = context;
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context);
    this.metadata = DefaultMetadata.EMPTY;
  }

  public Metadata getMetadata() {
    return this.metadata;
  }

  public CompletionStage<Void> addContactPoints(Set<InetSocketAddress> contactPoints) {
    if (contactPoints == null || contactPoints.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    } else {
      LOG.debug("Adding initial contact points {}", contactPoints);
      CompletableFuture<Void> initNodesFuture = new CompletableFuture<>();
      RunOrSchedule.on(
          adminExecutor, () -> singleThreaded.initNodes(contactPoints, initNodesFuture));
      return initNodesFuture;
    }
  }

  public CompletionStage<Void> refreshNodes() {
    return context
        .topologyMonitor()
        .refreshNodeList()
        .thenApplyAsync(singleThreaded::refreshNodes, adminExecutor);
  }

  public void addNode(InetSocketAddress address) {
    context
        .topologyMonitor()
        .refreshNode(address)
        .thenApplyAsync(singleThreaded::addNode, adminExecutor)
        .exceptionally(
            e -> {
              LOG.debug(
                  "Error adding node "
                      + address
                      + ", this will be retried on the next full refresh",
                  e);
              return null;
            });
  }

  public void removeNode(InetSocketAddress address) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.removeNode(address));
  }

  public void refreshSchema(
      SchemaElementKind kind, String keyspace, String object, List<String> arguments) {
    // TODO refresh schema metadata
  }

  // TODO user-controlled refresh, shutdown?

  private class SingleThreaded {
    private final InternalDriverContext context;

    private SingleThreaded(InternalDriverContext context) {
      this.context = context;
    }

    private void initNodes(
        Set<InetSocketAddress> addresses, CompletableFuture<Void> initNodesFuture) {
      assert adminExecutor.inEventLoop();
      metadata = metadata.initNodes(addresses);
      initNodesFuture.complete(null);
    }

    private Void refreshNodes(Iterable<TopologyMonitor.NodeInfo> nodeInfos) {
      metadata = metadata.refreshNodes(nodeInfos);
      // TODO init LBP if needed
      return null;
    }

    private Void addNode(TopologyMonitor.NodeInfo nodeInfo) {
      //TODO
      return null;
    }

    private void removeNode(InetSocketAddress address) {
      LOG.debug("Removing node {}", address);
      metadata = metadata.removeNode(address);
    }
  }
}
