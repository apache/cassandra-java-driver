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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A listener that gets notified when nodes states change.
 *
 * <p>Implementations of this interface can be registered either via the configuration (see {@code
 * reference.conf} in the manual or core driver JAR), or programmatically via {@link
 * SessionBuilder#addNodeStateListener(NodeStateListener)}.
 *
 * <p>Note that the methods defined by this interface will be executed by internal driver threads,
 * and are therefore expected to have short execution times. If you need to perform long
 * computations or blocking calls in response to schema change events, it is strongly recommended to
 * schedule them asynchronously on a separate thread provided by your application code.
 *
 * <p>If you implement this interface but don't need to implement all the methods, extend {@link
 * NodeStateListenerBase}.
 *
 * <p>If your implementation of this interface requires access to a fully-initialized session,
 * consider wrapping it in a {@link SafeInitNodeStateListener}.
 */
public interface NodeStateListener extends AutoCloseable {

  /**
   * Invoked when a node is first added to the cluster.
   *
   * <p>The node is not up yet at this point. {@link #onUp(Node)} will be notified later if the
   * driver successfully connects to the node (provided that a session is opened and the node is not
   * {@link NodeDistance#IGNORED ignored}), or receives a topology event for it.
   *
   * <p>This method is <b>not</b> invoked for the contact points provided at initialization. It is
   * however for new nodes discovered during the full node list refresh after the first connection.
   */
  void onAdd(@NonNull Node node);

  /** Invoked when a node's state switches to {@link NodeState#UP}. */
  void onUp(@NonNull Node node);

  /**
   * Invoked when a node's state switches to {@link NodeState#DOWN} or {@link
   * NodeState#FORCED_DOWN}.
   */
  void onDown(@NonNull Node node);

  /**
   * Invoked when a node leaves the cluster.
   *
   * <p>This can be triggered by a topology event, or during a full node list refresh if the node is
   * absent from the new list.
   */
  void onRemove(@NonNull Node node);

  /**
   * Invoked when the session is ready to process user requests.
   *
   * <p>This corresponds to the moment when {@link SessionBuilder#build()} returns, or the future
   * returned by {@link SessionBuilder#buildAsync()} completes. If the session initialization fails,
   * this method will not get called.
   *
   * <p>Listener methods are invoked from different threads; if you store the session in a field,
   * make it at least volatile to guarantee proper publication.
   *
   * <p>Note that this method will not be the first one invoked on the listener; the driver emits
   * node events before that, during the initialization of the session:
   *
   * <ul>
   *   <li>First the driver shuffles the contact points, and tries each one sequentially. For any
   *       contact point that can't be reached, {@link #onDown(Node)} is invoked; for the one that
   *       eventually succeeds, {@link #onUp(Node)} is invoked and that node becomes the control
   *       node (if none succeeds, the session initialization fails and the process stops here).
   *   <li>The control node's {@code system.peers} table is inspected to discover the remaining
   *       nodes in the cluster. For any node that wasn't already a contact point, {@link
   *       #onAdd(Node)} is invoked; for any contact point that doesn't have a corresponding entry
   *       in the table, {@link #onRemove(Node)} is invoked;
   *   <li>The load balancing policy computes the nodes' {@linkplain NodeDistance distances}, and,
   *       for each LOCAL or REMOTE node, the driver creates a connection pool. If at least one
   *       pooled connection can be established, {@link #onUp(Node)} is invoked; otherwise, {@link
   *       #onDown(Node)} is invoked (no additional event is emitted for the control node, it is
   *       considered up since we already have a connection to it).
   *   <li>Once all the pools are created, the session is fully initialized and this method is
   *       invoked.
   * </ul>
   *
   * If you're not interested in those init events, or want to delay them until after the session is
   * ready, take a look at {@link SafeInitNodeStateListener}.
   *
   * <p>This method's default implementation is empty.
   */
  default void onSessionReady(@NonNull Session session) {}
}
