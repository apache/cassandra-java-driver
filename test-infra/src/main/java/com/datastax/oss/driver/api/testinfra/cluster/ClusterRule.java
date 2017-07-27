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
package com.datastax.oss.driver.api.testinfra.cluster;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.DefaultCluster;
import com.datastax.oss.driver.internal.testinfra.cluster.TestConfigLoader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A rule for creating and managing lifecycle of {@link Cluster} instances. A default cluster is
 * created on set up which the user can use to create new {@link Session}s with using {@link
 * #newSession()}. New {@link Cluster} instances can be created using the cluster methods provided
 * any {@link Cluster} created in this way will be closed when the rule is cleaned up.
 */
public class ClusterRule extends CassandraResourceRule {

  // the ccm rule to depend on
  private final CassandraResourceRule cassandraResource;

  // the default cluster that is auto created for this rule.
  private Cluster defaultCluster;

  // the default session that is auto created for this rule and is tied to the given keyspace.
  private Session defaultSession;

  // clusters created by this rule.
  private final Collection<Cluster> clusters = new ArrayList<>();

  private final String[] defaultClusterOptions;

  private static final AtomicInteger keyspaceId = new AtomicInteger();

  private final String keyspace = "ks_" + keyspaceId.getAndIncrement();

  private DriverConfigProfile slowProfile;

  private boolean createDefaultCluster;
  private boolean createDefaultSession;

  /**
   * Creates a ClusterRule wrapping the provided resource.
   *
   * @param cassandraResource resource to create clusters for.
   * @param options The config options to pass to the default created cluster.
   */
  public ClusterRule(CassandraResourceRule cassandraResource, String... options) {
    this(cassandraResource, true, true, options);
  }

  /**
   * Creates a ClusterRule wrapping the provided resource.
   *
   * @param cassandraResource resource to create clusters for.
   * @param createDefaultCluster whether or not to create a default cluster on initialization.
   * @param createDefaultSession whether or not to create a default session on initialization.
   * @param options The config options to pass to the default created cluster.
   */
  public ClusterRule(
      CassandraResourceRule cassandraResource,
      boolean createDefaultCluster,
      boolean createDefaultSession,
      String... options) {
    this.cassandraResource = cassandraResource;
    this.defaultClusterOptions = options;
    this.createDefaultCluster = createDefaultCluster;
    this.createDefaultSession = createDefaultSession;
  }

  @Override
  protected void before() {
    // ensure resource is initialized before initializing the defaultCluster.
    cassandraResource.setUp();
    if (createDefaultCluster) {
      defaultCluster = defaultCluster(defaultClusterOptions);
      clusters.add(defaultCluster);

      slowProfile =
          defaultCluster
              .getContext()
              .config()
              .getDefaultProfile()
              .withString(CoreDriverOption.REQUEST_TIMEOUT, "30s");
      // TODO: Make this more pleasant
      if (createDefaultSession) {
        if (!(cassandraResource instanceof SimulacronRule)) {
          createKeyspace();
          defaultSession = defaultCluster.connect(CqlIdentifier.fromCql(keyspace));
        } else {
          defaultSession = defaultCluster.connect();
        }
      }
    }
  }

  public void createKeyspace() {
    createKeyspace(defaultCluster);
  }

  public void createKeyspace(Cluster defaultCluster) {
    try (Session session = defaultCluster.connect()) {
      SimpleStatement createKeyspace =
          SimpleStatement.builder(
                  String.format(
                      "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
                      keyspace))
              .withConfigProfile(slowProfile)
              .build();
      session.execute(createKeyspace);
    }
  }

  private void dropKeyspace() {
    if (createDefaultSession) {
      defaultSession.execute(
          SimpleStatement.builder(String.format("DROP KEYSPACE IF EXISTS %s", keyspace))
              .withConfigProfile(slowProfile)
              .build());
    }
  }

  @Override
  protected void after() {
    if (createDefaultCluster) {
      if (!(cassandraResource instanceof SimulacronRule)) {
        dropKeyspace();
      }
    }
    clusters.forEach(
        c -> {
          if (!c.closeFuture().toCompletableFuture().isDone()) {
            c.close();
          }
        });
  }

  /** @return the default cluster created with this rule. */
  public Cluster cluster() {
    return defaultCluster;
  }

  /** @return the default session created with this rule. */
  public Session session() {
    return defaultSession;
  }

  /** @return keyspace associated with this rule. */
  public String keyspace() {
    return keyspace;
  }

  /** @return a config profile where the request timeout is 30 seconds. * */
  public DriverConfigProfile slowProfile() {
    return slowProfile;
  }

  /**
   * @return A {@link DefaultCluster} instance using the nodes in 0th DC as contact points and
   *     defaults for all other configuration. Registers the returned cluster with the rule so it is
   *     closed on completion.
   */
  public Cluster defaultCluster(String... options) {
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(getContactPoints())
            .withConfigLoader(new TestConfigLoader(options))
            .build();
    clusters.add(cluster);
    return cluster;
  }

  @Override
  public ProtocolVersion getHighestProtocolVersion() {
    return cassandraResource.getHighestProtocolVersion();
  }

  @Override
  public Set<InetSocketAddress> getContactPoints() {
    return cassandraResource.getContactPoints();
  }

  /** @return a new session from the default cluster associated with this rule. */
  public Session newSession() {
    return defaultCluster.connect();
  }
}
