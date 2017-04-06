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

import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetSocketAddress;
import java.util.Objects;

/**
 * An event emitted from the {@link TopologyMonitor}, indicating a change in the topology of the
 * Cassandra cluster.
 *
 * <p>As shown by the names, most of these events are mere suggestions, that the driver might choose
 * to ignore if they contradict other information it has about the nodes; see the documentation of
 * each factory method for detailed explanations.
 */
public class TopologyEvent {

  public enum Type {
    SUGGEST_UP,
    SUGGEST_DOWN,
    FORCE_UP,
    FORCE_DOWN,
    SUGGEST_ADDED,
    SUGGEST_REMOVED,
  }

  /**
   * Suggests that a node is up.
   *
   * <ul>
   *   <li>if the node is currently ignored by the driver's load balancing policy, this is reflected
   *       in the driver metadata's corresponding {@link Node}, for information purposes only.
   *   <li>otherwise:
   *       <ul>
   *         <li>if the driver already had active connections to that node, this has no effect.
   *         <li>if the driver was currently reconnecting to the node, this causes the current
   *             {@link
   *             com.datastax.oss.driver.api.core.connection.ReconnectionPolicy.ReconnectionSchedule}
   *             to be reset, and the next reconnection attempt to happen immediately.
   *       </ul>
   *
   * </ul>
   */
  public static TopologyEvent suggestUp(InetSocketAddress address) {
    return new TopologyEvent(Type.SUGGEST_UP, address);
  }

  /**
   * Suggests that a node is down.
   *
   * <ul>
   *   <li>if the node is currently ignored by the driver's load balancing policy, this is reflected
   *       in the driver metadata's corresponding {@link Node}, for information purposes only.
   *   <li>otherwise, if the driver still has at least one active connection to that node, this is
   *       ignored. In other words, a functioning connection is considered a more reliable
   *       indication than a topology event.
   *       <p>If you want to bypass that behavior and force the node down, use {@link
   *       #forceDown(InetSocketAddress)}.
   * </ul>
   */
  public static TopologyEvent suggestDown(InetSocketAddress address) {
    return new TopologyEvent(Type.SUGGEST_DOWN, address);
  }

  /**
   * Forces the driver to set a node down.
   *
   * <ul>
   *   <li>if the node is currently ignored by the driver's load balancing policy, this is reflected
   *       in the driver metadata, for information purposes only.
   *   <li>otherwise, all active connections to the node are closed, and any active reconnection is
   *       cancelled.
   * </ul>
   *
   * In all cases, the driver will never try to reconnect to the node again. If you decide to
   * reconnect to it later, use {@link #forceUp(InetSocketAddress)}</b>.
   *
   * <p>This is intended for deployments that use a custom {@link TopologyMonitor} (for example if
   * you do some kind of maintenance on a live node). This is also used internally by the driver
   * when it detects an unrecoverable error, such as a node that does not support the current
   * protocol version.
   */
  public static TopologyEvent forceDown(InetSocketAddress address) {
    return new TopologyEvent(Type.FORCE_DOWN, address);
  }

  /**
   * Cancels a previous {@link #forceDown(InetSocketAddress)} event for the node.
   *
   * <p>The node will be set back UP. If it is not ignored by the load balancing policy, a
   * connection pool will be reopened.
   */
  public static TopologyEvent forceUp(InetSocketAddress address) {
    return new TopologyEvent(Type.FORCE_UP, address);
  }

  /**
   * Suggests that a new node was added in the cluster.
   *
   * <p>The driver will ignore this event if the node is already present in its metadata, or if
   * information about the node can't be refreshed (i.e. {@link
   * TopologyMonitor#refreshNode(InetSocketAddress)} fails).
   */
  public static TopologyEvent suggestAdded(InetSocketAddress address) {
    return new TopologyEvent(Type.SUGGEST_ADDED, address);
  }

  /**
   * Suggests that a node was removed from the cluster.
   *
   * <p>The driver ignore this event if the node does not exist in its metadata.
   */
  public static TopologyEvent suggestRemoved(InetSocketAddress address) {
    return new TopologyEvent(Type.SUGGEST_REMOVED, address);
  }

  public final Type type;
  public final InetSocketAddress address;

  /** Builds a new instance (the static methods in this class are a preferred alternative). */
  public TopologyEvent(Type type, InetSocketAddress address) {
    this.type = type;
    this.address = address;
  }

  public boolean isForceEvent() {
    return type == Type.FORCE_DOWN || type == Type.FORCE_UP;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TopologyEvent) {
      TopologyEvent that = (TopologyEvent) other;
      return this.type == that.type && Objects.equals(this.address, that.address);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.type, this.address);
  }

  @Override
  public String toString() {
    return "TopologyEvent(" + type + ", " + address + ")";
  }
}
