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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.testinfra.CassandraResourceRule;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.NodeFilterToDistanceEvaluatorAdapter;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to manage {@link Session} instances manually.
 *
 * <p>Use this if you need to initialize a new session instance in each test method:
 *
 * <pre>{@code
 * public static @ClassRule CcmRule server = CcmRule.getInstance();
 *
 * // Or: public static @ClassRule SimulacronRule server =
 * //    new SimulacronRule(ClusterSpec.builder().withNodes(3));
 *
 * public void @Test should_do_something() {
 *   try (Session session = TestUtils.newSession(server)) {
 *     session.execute("some query");
 *   }
 * }
 * }</pre>
 *
 * The instances returned by {@code newSession()} methods are not managed automatically, you need to
 * close them yourself (this is done with a try-with-resources block in the example above).
 *
 * <p>If you can share the same {@code Session} instance between all test methods, {@link
 * SessionRule} provides a simpler alternative.
 */
public class SessionUtils {

  public static final String SESSION_BUILDER_CLASS_PROPERTY = "session.builder";

  private static final Logger LOG = LoggerFactory.getLogger(SessionUtils.class);
  private static final AtomicInteger keyspaceId = new AtomicInteger();
  private static final String DEFAULT_SESSION_CLASS_NAME = CqlSession.class.getName();

  private static String getSessionBuilderClass() {
    return System.getProperty(SESSION_BUILDER_CLASS_PROPERTY, DEFAULT_SESSION_CLASS_NAME);
  }

  @SuppressWarnings("unchecked")
  public static <SessionT extends Session> SessionBuilder<?, SessionT> baseBuilder() {
    String sessionBuilderClass = getSessionBuilderClass();
    try {
      Class<?> clazz = Class.forName(sessionBuilderClass);
      Method m = clazz.getMethod("builder");
      return (SessionBuilder<?, SessionT>) m.invoke(null);
    } catch (Exception e) {
      LOG.warn(
          "Could not construct SessionBuilder from {} using builder(), using default "
              + "implementation.",
          sessionBuilderClass,
          e);
      return (SessionBuilder<?, SessionT>) CqlSession.builder();
    }
  }

  public static ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    String sessionBuilderClass = getSessionBuilderClass();
    try {
      Class<?> clazz = Class.forName(sessionBuilderClass);
      Method m = clazz.getMethod("configLoaderBuilder");
      return (ProgrammaticDriverConfigLoaderBuilder) m.invoke(null);
    } catch (Exception e) {
      if (!sessionBuilderClass.equals(DEFAULT_SESSION_CLASS_NAME)) {
        LOG.warn(
            "Could not construct ProgrammaticDriverConfigLoaderBuilder from {} using "
                + "configLoaderBuilder(), using default implementation.",
            sessionBuilderClass,
            e);
      }
      return DriverConfigLoader.programmaticBuilder();
    }
  }

  /**
   * Creates a new instance of the driver's default {@code Session} implementation, using the nodes
   * in the 0th DC of the provided Cassandra resource as contact points, and the default
   * configuration augmented with the provided options.
   */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResource) {
    return newSession(cassandraResource, null, null);
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResource, CqlIdentifier keyspace) {
    return newSession(cassandraResource, keyspace, null, null, null);
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResourceRule, DriverConfigLoader loader) {
    return newSession(cassandraResourceRule, null, null, null, null, loader);
  }

  @SuppressWarnings("TypeParameterUnusedInFormals")
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResourceRule,
      CqlIdentifier keyspace,
      DriverConfigLoader loader) {
    return newSession(cassandraResourceRule, keyspace, null, null, null, loader);
  }

  private static <SessionT extends Session> SessionBuilder<?, SessionT> builder(
      CassandraResourceRule cassandraResource,
      CqlIdentifier keyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      Predicate<Node> nodeFilter) {
    SessionBuilder<?, SessionT> builder = baseBuilder();
    builder
        .addContactEndPoints(cassandraResource.getContactPoints())
        .withKeyspace(keyspace)
        .withNodeStateListener(nodeStateListener)
        .withSchemaChangeListener(schemaChangeListener);
    if (nodeFilter != null) {
      builder.withNodeDistanceEvaluator(new NodeFilterToDistanceEvaluatorAdapter(nodeFilter));
    }
    return builder;
  }

  @SuppressWarnings({"TypeParameterUnusedInFormals"})
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResource,
      CqlIdentifier keyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      Predicate<Node> nodeFilter) {
    SessionBuilder<?, SessionT> builder =
        builder(cassandraResource, keyspace, nodeStateListener, schemaChangeListener, nodeFilter);
    return builder.build();
  }

  @SuppressWarnings({"unchecked", "TypeParameterUnusedInFormals"})
  public static <SessionT extends Session> SessionT newSession(
      CassandraResourceRule cassandraResource,
      CqlIdentifier keyspace,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      Predicate<Node> nodeFilter,
      DriverConfigLoader loader) {
    SessionBuilder<?, SessionT> builder =
        builder(cassandraResource, keyspace, nodeStateListener, schemaChangeListener, nodeFilter);
    return (SessionT) builder.withConfigLoader(loader).build();
  }

  /**
   * Generates a keyspace identifier that is guaranteed to be unique in the current classloader.
   *
   * <p>This is useful to isolate tests that share a common server resource.
   */
  public static CqlIdentifier uniqueKeyspaceId() {
    return CqlIdentifier.fromCql("ks_" + keyspaceId.getAndIncrement());
  }

  /** Creates a keyspace through the given session instance, with the given profile. */
  public static void createKeyspace(
      Session session, CqlIdentifier keyspace, DriverExecutionProfile profile) {
    SimpleStatement createKeyspace =
        SimpleStatement.builder(
                String.format(
                    "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
                    keyspace.asCql(false)))
            .setExecutionProfile(profile)
            .build();
    session.execute(createKeyspace, Statement.SYNC);
  }

  /**
   * Calls {@link #createKeyspace(Session, CqlIdentifier, DriverExecutionProfile)} with {@link
   * #slowProfile(Session)} as the third argument.
   *
   * <p>Note that this creates a derived profile for each invocation, which has a slight performance
   * overhead. Instead, consider building the profile manually with {@link #slowProfile(Session)},
   * and storing it in a local variable so it can be reused.
   */
  public static void createKeyspace(Session session, CqlIdentifier keyspace) {
    createKeyspace(session, keyspace, slowProfile(session));
  }

  /** Drops a keyspace through the given session instance, with the given profile. */
  public static void dropKeyspace(
      Session session, CqlIdentifier keyspace, DriverExecutionProfile profile) {
    session.execute(
        SimpleStatement.builder(String.format("DROP KEYSPACE IF EXISTS %s", keyspace.asCql(false)))
            .setExecutionProfile(profile)
            .build(),
        Statement.SYNC);
  }

  /**
   * Calls {@link #dropKeyspace(Session, CqlIdentifier, DriverExecutionProfile)} with {@link
   * #slowProfile(Session)} as the third argument.
   *
   * <p>Note that this creates a derived profile for each invocation, which has a slight performance
   * overhead. Instead, consider building the profile manually with {@link #slowProfile(Session)},
   * and storing it in a local variable so it can be reused.
   */
  public static void dropKeyspace(Session session, CqlIdentifier keyspace) {
    dropKeyspace(session, keyspace, slowProfile(session));
  }

  /**
   * Builds a profile derived from the given cluster's default profile, with a higher request
   * timeout (30 seconds) that is appropriate for DML operations.
   */
  public static DriverExecutionProfile slowProfile(Session session) {
    return session
        .getContext()
        .getConfig()
        .getDefaultProfile()
        .withString(DefaultDriverOption.REQUEST_TIMEOUT, "30s");
  }
}
