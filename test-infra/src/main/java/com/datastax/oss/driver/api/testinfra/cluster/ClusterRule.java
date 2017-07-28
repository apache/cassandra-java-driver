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
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import org.junit.rules.ExternalResource;

/**
 * Creates and manages a {@link Cluster} instance for a test.
 *
 * <p>Use it in conjunction with a {@link CassandraResourceRule} that creates the server resource to
 * connect to:
 *
 * <pre>{@code
 * public static @ClassRule CcmRule server = CcmRule.getInstance();
 *
 * // Or: public static @ClassRule SimulacronRule server =
 * //    new SimulacronRule(ClusterSpec.builder().withNodes(3));
 *
 * public static @ClassRule ClusterRule cluster = new ClusterRule(server);
 *
 * public void @Test should_do_something() {
 *   cluster.session().execute("some query");
 * }
 * }</pre>
 *
 * Optionally, it can also create a dedicated keyspace (useful to isolate tests that share a common
 * server), and initialize a session.
 *
 * <p>If you would rather create a new keyspace manually in each test, see the utility methods in
 * {@link ClusterUtils}.
 */
public class ClusterRule extends ExternalResource {

  // the CCM or Simulacron rule to depend on
  private final CassandraResourceRule cassandraResource;
  private final CqlIdentifier keyspace;
  private final boolean createDefaultSession;
  private final String[] defaultClusterOptions;

  // the default cluster that is auto created for this rule.
  private Cluster cluster;

  // the default session that is auto created for this rule and is tied to the given keyspace.
  private Session defaultSession;

  private DriverConfigProfile slowProfile;

  /**
   * Returns a builder to construct an instance with a fluent API.
   *
   * @param cassandraResource resource to create clusters for.
   */
  public static ClusterRuleBuilder builder(CassandraResourceRule cassandraResource) {
    return new ClusterRuleBuilder(cassandraResource);
  }

  /** @see #builder(CassandraResourceRule) */
  public ClusterRule(CassandraResourceRule cassandraResource, String... options) {
    this(cassandraResource, true, true, options);
  }

  /** @see #builder(CassandraResourceRule) */
  public ClusterRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      boolean createDefaultSession,
      String... options) {
    this.cassandraResource = cassandraResource;
    this.keyspace =
        (cassandraResource instanceof SimulacronRule || !createKeyspace)
            ? null
            : ClusterUtils.uniqueKeyspaceId();
    this.createDefaultSession = createDefaultSession;
    this.defaultClusterOptions = options;
  }

  @Override
  protected void before() {
    // ensure resource is initialized before initializing the defaultCluster.
    cassandraResource.setUp();
    cluster = ClusterUtils.newCluster(cassandraResource, defaultClusterOptions);

    slowProfile = ClusterUtils.slowProfile(cluster);

    if (keyspace != null) {
      ClusterUtils.createKeyspace(cluster, keyspace, slowProfile);
    }
    if (createDefaultSession) {
      defaultSession = cluster.connect(keyspace);
    }
  }

  @Override
  protected void after() {
    if (keyspace != null) {
      ClusterUtils.dropKeyspace(cluster, keyspace, slowProfile);
    }
    cluster.close();
  }

  /** @return the cluster created with this rule. */
  public Cluster cluster() {
    return cluster;
  }

  /**
   * @return the default session created with this rule, or {@code null} if no default session was
   *     created.
   */
  public Session session() {
    return defaultSession;
  }

  /**
   * @return the identifier of the keyspace associated with this rule, or {@code null} if no
   *     keyspace was created (this is always the case if the server resource is a {@link
   *     SimulacronRule}).
   */
  public CqlIdentifier keyspace() {
    return keyspace;
  }

  /** @return a config profile where the request timeout is 30 seconds. * */
  public DriverConfigProfile slowProfile() {
    return slowProfile;
  }
}
