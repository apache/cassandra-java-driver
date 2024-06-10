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
package com.datastax.oss.driver.api.testinfra.simulacron;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Inet4Resolver;
import com.datastax.oss.simulacron.server.Server;
import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SimulacronRule extends CassandraResourceRule {
  // TODO perhaps share server some other way
  // TODO: Temporarily do not release addresses to ensure IPs are always ordered
  // TODO: Add a way to configure the server for multiple nodes per ip
  public static final Server server =
      Server.builder()
          .withAddressResolver(
              new Inet4Resolver(9043) {
                @Override
                public void release(SocketAddress address) {}
              })
          .build();

  private final ClusterSpec clusterSpec;
  private BoundCluster boundCluster;

  private final AtomicBoolean started = new AtomicBoolean();

  public SimulacronRule(ClusterSpec clusterSpec) {
    this.clusterSpec = clusterSpec;
  }

  public SimulacronRule(ClusterSpec.Builder clusterSpec) {
    this(clusterSpec.build());
  }

  /**
   * Convenient fluent name for getting at bound cluster.
   *
   * @return default bound cluster for this simulacron instance.
   */
  public BoundCluster cluster() {
    return boundCluster;
  }

  public BoundCluster getBoundCluster() {
    return boundCluster;
  }

  @Override
  protected void before() {
    // prevent duplicate initialization of rule
    if (started.compareAndSet(false, true)) {
      boundCluster = server.register(clusterSpec);
    }
  }

  @Override
  protected void after() {
    boundCluster.close();
  }

  /** @return All nodes in first data center. */
  @Override
  public Set<EndPoint> getContactPoints() {
    return boundCluster.dc(0).getNodes().stream()
        .map(node -> new DefaultEndPoint(node.inetSocketAddress()))
        .collect(Collectors.toSet());
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    return DefaultProtocolVersion.V4;
  }
}
