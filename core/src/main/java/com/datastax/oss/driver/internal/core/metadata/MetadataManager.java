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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor.NodeInfo;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
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

  public CompletionStage<Void> refreshNode(Node node) {
    return context
        .topologyMonitor()
        .refreshNode(node)
        // The callback only updates volatile fields so no need to schedule it on adminExecutor
        .thenApply(
            maybeInfo -> {
              if (maybeInfo.isPresent()) {
                NodesRefresh.copyInfos(maybeInfo.get(), (DefaultNode) node);
              } else {
                LOG.debug(
                    "Topology monitor did not return any info for the refresh of {}, skipping",
                    node);
              }
              return null;
            });
  }

  public void addNode(InetSocketAddress address) {
    context
        .topologyMonitor()
        .getNewNodeInfo(address)
        .whenCompleteAsync(
            (info, error) -> {
              if (error != null) {
                LOG.debug(
                    "Error refreshing node info for "
                        + address
                        + ", this will be retried on the next full refresh",
                    error);
              } else {
                singleThreaded.addNode(address, info);
              }
            },
            adminExecutor);
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
      refresh(new InitContactPointsRefresh(metadata, addresses));
      initNodesFuture.complete(null);
    }

    private Void refreshNodes(Iterable<NodeInfo> nodeInfos) {
      return refresh(new FullNodeListRefresh(metadata, nodeInfos));
    }

    private void addNode(InetSocketAddress address, Optional<NodeInfo> maybeInfo) {
      try {
        if (maybeInfo.isPresent()) {
          NodeInfo info = maybeInfo.get();
          if (!address.equals(info.getConnectAddress())) {
            // This would be a bug in the TopologyMonitor, protect against it
            LOG.warn(
                "Received a request to add a node for {}, "
                    + "but the provided info uses the address {}, ignoring it",
                address,
                info.getBroadcastAddress());
          } else {
            refresh(new AddNodeRefresh(metadata, info));
          }
        } else {
          LOG.debug(
              "Ignoring node addition for {} because the "
                  + "topology monitor didn't return any information",
              address);
        }
      } catch (Throwable t) {
        LOG.warn("Unexpected exception while handling added node", t);
      }
    }

    private void removeNode(InetSocketAddress address) {
      refresh(new RemoveNodeRefresh(metadata, address));
    }
  }

  @VisibleForTesting
  Void refresh(MetadataRefresh refresh) {
    assert adminExecutor.inEventLoop();
    refresh.compute();
    metadata = refresh.newMetadata;
    for (Object event : refresh.events) {
      context.eventBus().fire(event);
    }
    return null;
  }
}
