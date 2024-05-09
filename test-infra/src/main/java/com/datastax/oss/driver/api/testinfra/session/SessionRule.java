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
package com.datastax.oss.driver.api.testinfra.session;

import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.ccm.BaseCcmRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import java.util.Objects;
import java.util.Optional;
import org.junit.rules.ExternalResource;

/**
 * Creates and manages a {@link Session} instance for a test.
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
 * public static @ClassRule SessionRule sessionRule = new SessionRule(server);
 *
 * public void @Test should_do_something() {
 *   sessionRule.session().execute("some query");
 * }
 * }</pre>
 *
 * Optionally, it can also create a dedicated keyspace (useful to isolate tests that share a common
 * server).
 *
 * <p>If you would rather create a new keyspace manually in each test, see the utility methods in
 * {@link SessionUtils}.
 */
public class SessionRule<SessionT extends Session> extends ExternalResource {

  private static final Version V6_8_0 = Objects.requireNonNull(Version.parse("6.8.0"));

  // the CCM or Simulacron rule to depend on
  private final CassandraResourceRule cassandraResource;
  private final NodeStateListener nodeStateListener;
  private final SchemaChangeListener schemaChangeListener;
  private final CqlIdentifier keyspace;
  private final DriverConfigLoader configLoader;
  private final String graphName;
  private final boolean isCoreGraph;

  // the session that is auto created for this rule and is tied to the given keyspace.
  private SessionT session;

  private DriverExecutionProfile slowProfile;

  /**
   * Returns a builder to construct an instance with a fluent API.
   *
   * @param cassandraResource resource to create clusters for.
   */
  public static CqlSessionRuleBuilder builder(CassandraResourceRule cassandraResource) {
    return new CqlSessionRuleBuilder(cassandraResource);
  }

  /** @see #builder(CassandraResourceRule) */
  public SessionRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      DriverConfigLoader configLoader,
      String graphName,
      boolean isCoreGraph) {
    this.cassandraResource = cassandraResource;
    this.nodeStateListener = nodeStateListener;
    this.schemaChangeListener = schemaChangeListener;
    this.keyspace =
        (cassandraResource instanceof SimulacronRule || !createKeyspace)
            ? null
            : SessionUtils.uniqueKeyspaceId();
    this.configLoader = configLoader;
    this.graphName = graphName;
    this.isCoreGraph = isCoreGraph;
  }

  public SessionRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      DriverConfigLoader configLoader,
      String graphName) {
    this(
        cassandraResource,
        createKeyspace,
        nodeStateListener,
        schemaChangeListener,
        configLoader,
        graphName,
        false);
  }

  public SessionRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      DriverConfigLoader configLoader) {
    this(
        cassandraResource,
        createKeyspace,
        nodeStateListener,
        schemaChangeListener,
        configLoader,
        null,
        false);
  }

  @Override
  protected void before() {
    session =
        SessionUtils.newSession(
            cassandraResource, null, nodeStateListener, schemaChangeListener, null, configLoader);
    slowProfile = SessionUtils.slowProfile(session);
    if (keyspace != null) {
      SessionUtils.createKeyspace(session, keyspace, slowProfile);
      session.execute(
          SimpleStatement.newInstance(String.format("USE %s", keyspace.asCql(false))),
          Statement.SYNC);
    }
    if (graphName != null) {
      Optional<Version> dseVersion =
          (cassandraResource instanceof BaseCcmRule)
              ? ((BaseCcmRule) cassandraResource).getDseVersion()
              : Optional.empty();
      if (!dseVersion.isPresent()) {
        throw new IllegalArgumentException("DseSessionRule should work with DSE.");
      }
      if (dseVersion.get().compareTo(V6_8_0) >= 0) {
        session()
            .execute(
                ScriptGraphStatement.newInstance(
                        String.format(
                            "system.graph('%s').ifNotExists()%s.create()",
                            this.graphName, isCoreGraph ? ".coreEngine()" : ".classicEngine()"))
                    .setSystemQuery(true),
                ScriptGraphStatement.SYNC);
      } else {
        if (isCoreGraph) {
          throw new IllegalArgumentException(
              "Core graph is not supported for DSE version < " + V6_8_0);
        }
        session()
            .execute(
                ScriptGraphStatement.newInstance(
                        String.format("system.graph('%s').ifNotExists().create()", this.graphName))
                    .setSystemQuery(true),
                ScriptGraphStatement.SYNC);
      }
    }
  }

  @Override
  protected void after() {
    if (graphName != null) {
      session()
          .execute(
              ScriptGraphStatement.newInstance(
                      String.format("system.graph('%s').drop()", this.graphName))
                  .setSystemQuery(true),
              ScriptGraphStatement.SYNC);
    }
    if (keyspace != null) {
      SessionUtils.dropKeyspace(session, keyspace, slowProfile);
    }
    session.close();
  }

  /** @return the session created with this rule. */
  public SessionT session() {
    return session;
  }

  /**
   * @return the identifier of the keyspace associated with this rule, or {@code null} if no
   *     keyspace was created (this is always the case if the server resource is a {@link
   *     SimulacronRule}).
   */
  public CqlIdentifier keyspace() {
    return keyspace;
  }

  public String getGraphName() {
    return graphName;
  }

  /** @return a config profile where the request timeout is 30 seconds. * */
  public DriverExecutionProfile slowProfile() {
    return slowProfile;
  }
}
