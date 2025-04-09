/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.SessionLifecycleManager;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class DefaultSessionLifecycleManager implements SessionLifecycleManager {
  private final DefaultSession session;
  private final InternalDriverContext context;
  private CompletableFuture<Void> suspendFuture;
  private Map<Node, NodeState> lastState;

  public DefaultSessionLifecycleManager(DefaultSession session) {
    this.session = session;
    this.context = (InternalDriverContext) session.getContext();
  }

  @Override
  public synchronized CompletionStage<Void> suspendAsync() {
    if (suspendFuture != null) {
      return suspendFuture;
    }
    suspendFuture = new CompletableFuture<>();
    // ControlConnection would try to reconnect when it receives the event that
    // node was brought down; closing the very channel to this node prevents that.
    this.context
        .getControlConnection()
        .channel()
        .close()
        .addListener(
            f -> {
              if (f.isSuccess()) {
                forceNodesDown()
                    .whenComplete(
                        (ignored, throwable) -> {
                          if (throwable != null) {
                            suspendFuture.completeExceptionally(throwable);
                          } else {
                            suspendFuture.complete(null);
                          }
                        });
              } else {
                suspendFuture.completeExceptionally(f.cause());
              }
            });
    return suspendFuture;
  }

  private CompletionStage<Void> forceNodesDown() {
    lastState = new HashMap<>();
    ArrayList<CompletionStage<Void>> closeFutures = new ArrayList<>();
    for (Map.Entry<Node, ChannelPool> e : session.getPools().entrySet()) {
      Node node = e.getKey();
      NodeState currentState = node.getState();
      lastState.put(node, currentState);
      closeFutures.add(e.getValue().closeFuture());
      context
          .getEventBus()
          .fire(NodeStateEvent.changed(currentState, NodeState.FORCED_DOWN, (DefaultNode) node));
    }
    return CompletableFutures.allDone(closeFutures);
  }

  @Override
  public void resume() {
    if (suspendFuture == null) {
      return;
    }
    suspendFuture.whenComplete(
        (ignored, throwable) -> {
          if (throwable != null || lastState == null) {
            return;
          }
          synchronized (this) {
            for (Map.Entry<Node, NodeState> e : lastState.entrySet()) {
              NodeStateEvent changed =
                  NodeStateEvent.changed(
                      NodeState.FORCED_DOWN, e.getValue(), (DefaultNode) e.getKey());
              this.context.getEventBus().fire(changed);
            }
            lastState = null;
            suspendFuture = null;
            context.getControlConnection().reconnectNow();
          }
        });
  }

  @Override
  public synchronized boolean isSuspended() {
    return suspendFuture != null;
  }
}
