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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

/**
 * Implementation note: all the mutable state in this class is read concurrently, but only mutated
 * from {@link MetadataManager}'s admin thread.
 */
@ThreadSafe
public class DefaultNode implements Node, Serializable {

  private static final long serialVersionUID = 1;

  private volatile EndPoint endPoint;
  // A deserialized node is not attached to a session anymore, so we don't need to retain this
  private transient volatile NodeMetricUpdater metricUpdater;

  volatile InetSocketAddress broadcastRpcAddress;
  volatile InetSocketAddress broadcastAddress;
  volatile InetSocketAddress listenAddress;
  volatile String datacenter;
  volatile String rack;
  volatile Version cassandraVersion;
  // Keep a copy of the raw tokens, to detect if they have changed when we refresh the node
  volatile Set<String> rawTokens;
  volatile Map<String, Object> extras;
  volatile UUID hostId;
  volatile UUID schemaVersion;

  // These 4 fields are read concurrently, but only mutated on NodeStateManager's admin thread
  volatile NodeState state;
  volatile int openConnections;
  volatile int reconnections;
  volatile long upSinceMillis;

  volatile NodeDistance distance;

  public DefaultNode(EndPoint endPoint, InternalDriverContext context) {
    this.endPoint = endPoint;
    this.state = NodeState.UNKNOWN;
    this.distance = NodeDistance.IGNORED;
    this.rawTokens = Collections.emptySet();
    this.extras = Collections.emptyMap();
    // We leak a reference to a partially constructed object (this), but in practice this won't be a
    // problem because the node updater only needs the connect address to initialize.
    this.metricUpdater = context.getMetricsFactory().newNodeUpdater(this);
    this.upSinceMillis = -1;
  }

  @NonNull
  @Override
  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(@NonNull EndPoint newEndPoint, @NonNull InternalDriverContext context) {
    if (!newEndPoint.equals(endPoint)) {
      endPoint = newEndPoint;

      // The endpoint is also used to build metric names, so make sure they get updated
      NodeMetricUpdater previousMetricUpdater = metricUpdater;
      if (!(previousMetricUpdater instanceof NoopNodeMetricUpdater)) {
        metricUpdater = context.getMetricsFactory().newNodeUpdater(this);
      }
    }
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastRpcAddress() {
    return Optional.ofNullable(broadcastRpcAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastAddress() {
    return Optional.ofNullable(broadcastAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getListenAddress() {
    return Optional.ofNullable(listenAddress);
  }

  @Nullable
  @Override
  public String getDatacenter() {
    return datacenter;
  }

  @Nullable
  @Override
  public String getRack() {
    return rack;
  }

  @Nullable
  @Override
  public Version getCassandraVersion() {
    return cassandraVersion;
  }

  @Nullable
  @Override
  public UUID getHostId() {
    return hostId;
  }

  @Nullable
  @Override
  public UUID getSchemaVersion() {
    return schemaVersion;
  }

  @NonNull
  @Override
  public Map<String, Object> getExtras() {
    return extras;
  }

  @NonNull
  @Override
  public NodeState getState() {
    return state;
  }

  @Override
  public long getUpSinceMillis() {
    return upSinceMillis;
  }

  @Override
  public int getOpenConnections() {
    return openConnections;
  }

  @Override
  public boolean isReconnecting() {
    return reconnections > 0;
  }

  @NonNull
  @Override
  public NodeDistance getDistance() {
    return distance;
  }

  public NodeMetricUpdater getMetricUpdater() {
    return metricUpdater;
  }

  @Override
  public String toString() {
    // Include the hash code because this class uses reference equality
    return String.format(
        "Node(endPoint=%s, hostId=%s, hashCode=%x, dc=%s)",
        getEndPoint(), getHostId(), hashCode(), getDatacenter());
  }

  /** Note: deliberately not exposed by the public interface. */
  public Set<String> getRawTokens() {
    return rawTokens;
  }
}
