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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.internal.testinfra.cluster.TestConfigLoader;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility methods to manage {@link Cluster} instances manually.
 *
 * <p>Use this if you need to initialize a new cluster instance in each test method:
 *
 * <pre>{@code
 * public static @ClassRule CcmRule server = CcmRule.getInstance();
 *
 * // Or: public static @ClassRule SimulacronRule server =
 * //    new SimulacronRule(ClusterSpec.builder().withNodes(3));
 *
 * public void @Test should_do_something() {
 *   try (Cluster cluster = TestUtils.newCluster(server)) {
 *     Session session = cluster.connect();
 *     session.execute("some query");
 *   }
 * }
 * }</pre>
 *
 * The instances returned by {@link #newCluster(CassandraResourceRule, String...)} are not managed
 * automatically, you need to close them yourself (this is done with a try-with-resources block in
 * the example above).
 *
 * <p>If you can share the same {@code Cluster} instance between all test methods, {@link
 * ClusterRule} provides a simpler alternative.
 */
public class ClusterUtils {
  private static final AtomicInteger keyspaceId = new AtomicInteger();

  /**
   * Creates a new instance of the driver's default {@code Cluster} implementation, using the nodes
   * in the 0th DC of the provided Cassandra resource as contact points, and the default
   * configuration augmented with the provided options.
   */
  public static Cluster<CqlSession> newCluster(
      CassandraResourceRule cassandraResource, String... options) {
    return Cluster.builder()
        .addContactPoints(cassandraResource.getContactPoints())
        .withConfigLoader(new TestConfigLoader(options))
        .build();
  }

  /**
   * Generates a keyspace identifier that is guaranteed to be unique in the current classloader.
   *
   * <p>This is useful to isolate tests that share a common server resource.
   */
  public static CqlIdentifier uniqueKeyspaceId() {
    return CqlIdentifier.fromCql("ks_" + keyspaceId.getAndIncrement());
  }

  /** Creates a keyspace through the given cluster instance, with the given profile. */
  public static void createKeyspace(
      Cluster<CqlSession> cluster, CqlIdentifier keyspace, DriverConfigProfile profile) {
    try (CqlSession session = cluster.connect()) {
      SimpleStatement createKeyspace =
          SimpleStatement.builder(
                  String.format(
                      "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
                      keyspace.asCql(false)))
              .withConfigProfile(profile)
              .build();
      session.execute(createKeyspace);
    }
  }

  /**
   * Calls {@link #createKeyspace(Cluster, CqlIdentifier, DriverConfigProfile)} with {@link
   * #slowProfile(Cluster)} as the third argument.
   *
   * <p>Note that this creates a derived profile for each invocation, which has a slight performance
   * overhead. Instead, consider building the profile manually with {@link #slowProfile(Cluster)},
   * and storing it in a local variable so it can be reused.
   */
  public static void createKeyspace(Cluster<CqlSession> cluster, CqlIdentifier keyspace) {
    createKeyspace(cluster, keyspace, slowProfile(cluster));
  }

  /** Drops a keyspace through the given cluster instance, with the given profile. */
  public static void dropKeyspace(
      Cluster<CqlSession> cluster, CqlIdentifier keyspace, DriverConfigProfile profile) {
    try (CqlSession session = cluster.connect()) {
      session.execute(
          SimpleStatement.builder(
                  String.format("DROP KEYSPACE IF EXISTS %s", keyspace.asCql(false)))
              .withConfigProfile(profile)
              .build());
    }
  }

  /**
   * Calls {@link #dropKeyspace(Cluster, CqlIdentifier, DriverConfigProfile)} with {@link
   * #slowProfile(Cluster)} as the third argument.
   *
   * <p>Note that this creates a derived profile for each invocation, which has a slight performance
   * overhead. Instead, consider building the profile manually with {@link #slowProfile(Cluster)},
   * and storing it in a local variable so it can be reused.
   */
  public static void dropKeyspace(Cluster<CqlSession> cluster, CqlIdentifier keyspace) {
    dropKeyspace(cluster, keyspace, slowProfile(cluster));
  }

  /**
   * Builds a profile derived from the given cluster's default profile, with a higher request
   * timeout (30 seconds) that is appropriate for DML operations.
   */
  public static DriverConfigProfile slowProfile(Cluster<CqlSession> cluster) {
    return cluster
        .getContext()
        .config()
        .getDefaultProfile()
        .withString(CoreDriverOption.REQUEST_TIMEOUT, "30s");
  }
}
