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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A listener that gets notified when nodes states change.
 *
 * <p>An implementation of this interface can be registered in the configuration, or with {@link
 * SessionBuilder#withNodeStateListener(NodeStateListener)}.
 *
 * <p>Note that the methods defined by this interface will be executed by internal driver threads,
 * and are therefore expected to have short execution times. If you need to perform long
 * computations or blocking calls in response to schema change events, it is strongly recommended to
 * schedule them asynchronously on a separate thread provided by your application code.
 *
 * <p>If you implement this interface but don't need to implement all the methods, extend {@link
 * NodeStateListenerBase}.
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
}
