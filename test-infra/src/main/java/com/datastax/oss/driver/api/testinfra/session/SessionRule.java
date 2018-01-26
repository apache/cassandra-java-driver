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
package com.datastax.oss.driver.api.testinfra.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
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

  // the CCM or Simulacron rule to depend on
  private final CassandraResourceRule cassandraResource;
  private final NodeStateListener[] nodeStateListeners;
  private final CqlIdentifier keyspace;
  private final String[] defaultOptions;

  // the session that is auto created for this rule and is tied to the given keyspace.
  private SessionT session;

  private DriverConfigProfile slowProfile;

  /**
   * Returns a builder to construct an instance with a fluent API.
   *
   * @param cassandraResource resource to create clusters for.
   */
  public static CqlSessionRuleBuilder builder(CassandraResourceRule cassandraResource) {
    return new CqlSessionRuleBuilder(cassandraResource);
  }

  /** @see #builder(CassandraResourceRule) */
  public SessionRule(CassandraResourceRule cassandraResource, String... options) {
    this(cassandraResource, true, new NodeStateListener[0], options);
  }

  /** @see #builder(CassandraResourceRule) */
  public SessionRule(
      CassandraResourceRule cassandraResource,
      boolean createKeyspace,
      NodeStateListener[] nodeStateListeners,
      String... options) {
    this.cassandraResource = cassandraResource;
    this.nodeStateListeners = nodeStateListeners;
    this.keyspace =
        (cassandraResource instanceof SimulacronRule || !createKeyspace)
            ? null
            : SessionUtils.uniqueKeyspaceId();
    this.defaultOptions = options;
  }

  @Override
  protected void before() {
    // ensure resource is initialized before initializing the session.
    cassandraResource.setUp();

    session = SessionUtils.newSession(cassandraResource, null, nodeStateListeners, defaultOptions);
    slowProfile = SessionUtils.slowProfile(session);
    if (keyspace != null) {
      SessionUtils.createKeyspace(session, keyspace, slowProfile);
      session.execute(
          SimpleStatement.newInstance(String.format("USE %s", keyspace.asCql(false))),
          Statement.SYNC);
    }
  }

  @Override
  protected void after() {
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

  /** @return a config profile where the request timeout is 30 seconds. * */
  public DriverConfigProfile slowProfile() {
    return slowProfile;
  }
}
