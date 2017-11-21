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
package com.datastax.oss.driver.api.testinfra.simulacron;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.AddressResolver;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class SimulacronRule extends CassandraResourceRule {
  // TODO perhaps share server some other way
  public static final Server server =
      Server.builder().withAddressResolver(new AddressResolver.Inet4Resolver(9043)).build();

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
  public Set<InetSocketAddress> getContactPoints() {
    return boundCluster
        .dc(0)
        .getNodes()
        .stream()
        .map(BoundNode::inetSocketAddress)
        .collect(Collectors.toSet());
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    return CoreProtocolVersion.V4;
  }
}
