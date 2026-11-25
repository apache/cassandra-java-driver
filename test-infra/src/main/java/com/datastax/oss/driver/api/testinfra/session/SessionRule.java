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
import com.datastax.oss.driver.api.testinfra.astra.BaseAstraRule;
import com.datastax.oss.driver.api.testinfra.ccm.BaseCcmRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.SchemaChangeSynchronizer;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import java.util.Objects;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(SessionRule.class);
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
    // Determine keyspace based on backend type:
    // - Simulacron: no keyspace (null)
    // - Astra: use shared keyspace from AstraBridge (when createKeyspace is true)
    // - CCM/other: generate unique keyspace (when createKeyspace is true)
    // - When createKeyspace is false: no keyspace (null)
    if (!createKeyspace || cassandraResource instanceof SimulacronRule) {
      this.keyspace = null;
    } else if (cassandraResource instanceof BaseAstraRule) {
      // For Astra, use the shared keyspace from AstraBridge
      BaseAstraRule astraRule = (BaseAstraRule) cassandraResource;
      String sharedKeyspace = astraRule.getAstraBridge().getKeyspace();
      this.keyspace = sharedKeyspace != null ? CqlIdentifier.fromCql(sharedKeyspace) : null;
    } else {
      // For CCM and other backends, generate a unique keyspace
      this.keyspace = SessionUtils.uniqueKeyspaceId();
    }
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
    // Create session without keyspace first
    session =
        SessionUtils.newSession(
            cassandraResource, null, nodeStateListener, schemaChangeListener, null, configLoader);

    slowProfile = SessionUtils.slowProfile(session);

    // Create keyspace if needed
    if (keyspace != null) {
      if (cassandraResource instanceof BaseAstraRule) {
        // For Astra, the shared keyspace already exists - just switch to it
        BaseAstraRule astraRule = (BaseAstraRule) cassandraResource;
        String sharedKeyspace = astraRule.getAstraBridge().getKeyspace();
        LOG.warn(
            "Using shared Astra keyspace: {} with CassandraResource: {}",
            sharedKeyspace,
            cassandraResource.getClass().getSimpleName());
      } else {
        // For CCM and other backends, create a unique keyspace using CQL
        LOG.warn(
            "Creating keyspace: {} with CassandraResource: {}",
            keyspace,
            cassandraResource.getClass().getSimpleName());
        SessionUtils.createKeyspace(session, keyspace, slowProfile);
      }
      // Switch to the keyspace
      session.execute(
          SimpleStatement.newInstance(String.format("USE %s", keyspace.asCql(false))),
          Statement.SYNC);
    }
    if (graphName != null) {
      BaseCcmRule rule =
          (cassandraResource instanceof BaseCcmRule) ? ((BaseCcmRule) cassandraResource) : null;
      if (rule == null || !CcmBridge.isDistributionOf(BackendType.DSE)) {
        throw new IllegalArgumentException("DseSessionRule should work with DSE.");
      }
      if (rule.getDistributionVersion().compareTo(V6_8_0) >= 0) {
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
    // Only drop keyspace for non-Astra resources (Astra keyspaces are managed by Astra)
    if (keyspace != null
        && !(cassandraResource
            instanceof com.datastax.oss.driver.api.testinfra.astra.BaseAstraRule)) {
      SchemaChangeSynchronizer.withLock(
          () -> {
            SessionUtils.dropKeyspace(session, keyspace, slowProfile);
          });
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
