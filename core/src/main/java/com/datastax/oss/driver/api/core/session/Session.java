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
package com.datastax.oss.driver.api.core.session;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.ResourceBundle;
import java.util.concurrent.CompletionStage;

/**
 * A nexus to send requests to a Cassandra cluster.
 *
 * <p>This is a high-level abstraction capable of handling arbitrary request and result types. For
 * CQL statements, {@link CqlSession} provides convenience methods with more familiar signatures (by
 * default, all the instances returned by the driver also implement {@link CqlSession}).
 *
 * <p>The driver's request execution logic is pluggable (see {@code RequestProcessor} in the
 * internal API). This is intended for future extensions, for example a reactive API for CQL
 * statements, or graph requests in the Datastax Enterprise driver. Hence the generic {@link
 * #execute(Request, GenericType)} method in this interface, that makes no assumptions about the
 * request or result type.
 *
 * @see CqlSession#builder()
 */
public interface Session extends AsyncAutoCloseable {

  /**
   * The current version of the core driver (in other words, the version of the {@code
   * com.datastax.oss:java-driver-core} artifact).
   *
   * <p>This is intended for products that wrap or extend the driver, as a way to check
   * compatibility if end-users override the driver version in their application.
   */
  static String getCoreDriverVersion() {
    // Note: getBundle caches its result
    return ResourceBundle.getBundle("com.datastax.oss.driver.Driver").getString("driver.version");
  }

  /**
   * The unique name identifying this client.
   *
   * @see CoreDriverOption#SESSION_NAME
   */
  String getName();

  /**
   * Returns a snapshot of the Cassandra cluster's topology and schema metadata.
   *
   * <p>In order to provide atomic updates, this method returns an immutable object: the node list,
   * token map, and schema contained in a given instance will always be consistent with each other
   * (but note that {@link Node} itself is not immutable: some of its properties will be updated
   * dynamically, in particular {@link Node#getState()}).
   *
   * <p>As a consequence of the above, you should call this method each time you need a fresh view
   * of the metadata. <b>Do not</b> call it once and store the result, because it is a frozen
   * snapshot that will become stale over time.
   *
   * <p>If a metadata refresh triggers events (such as node added/removed, or schema events), then
   * the new version of the metadata is guaranteed to be visible by the time you receive these
   * events.
   */
  Metadata getMetadata();

  /** Whether schema metadata is currently enabled. */
  boolean isSchemaMetadataEnabled();

  /**
   * Enable or disable schema metadata programmatically.
   *
   * <p>Use this method to override the value defined in the driver's configuration; one typical use
   * case is to temporarily disable schema metadata while the client issues a sequence of DDL
   * statements.
   *
   * <p>If calling this method re-enables the metadata (that is, {@link #isSchemaMetadataEnabled()}
   * was false before, and becomes true as a result of the call), a refresh is also triggered.
   *
   * @param newValue a boolean value to enable or disable schema metadata programmatically, or
   *     {@code null} to use the driver's configuration.
   * @see CoreDriverOption#METADATA_SCHEMA_ENABLED
   * @return if this call triggered a refresh, a future that will complete when that refresh is
   *     complete. Otherwise, a completed future with the current metadata.
   */
  CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue);

  /**
   * Force an immediate refresh of the schema metadata, even if it is currently disabled (either in
   * the configuration or via {@link #setSchemaMetadataEnabled(Boolean)}).
   *
   * <p>The new metadata is returned in the resulting future (and will also be reflected by {@link
   * #getMetadata()} when that future completes).
   */
  CompletionStage<Metadata> refreshSchemaAsync();

  /**
   * Convenience method to call {@link #refreshSchemaAsync()} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default Metadata refreshSchema() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(refreshSchemaAsync());
  }

  /**
   * Checks if all nodes in the cluster agree on a common schema version.
   *
   * <p>Due to the distributed nature of Cassandra, schema changes made on one node might not be
   * immediately visible to others. Under certain circumstances, the driver waits until all nodes
   * agree on a common schema version (namely: before a schema refresh, and before completing a
   * successful schema-altering query). To do so, it queries system tables to find out the schema
   * version of all nodes that are currently {@link NodeState#UP UP}. If all the versions match, the
   * check succeeds, otherwise it is retried periodically, until a given timeout (specified in the
   * configuration).
   *
   * <p>A schema agreement failure is not fatal, but it might produce unexpected results (for
   * example, getting an "unconfigured table" error for a table that you created right before, just
   * because the two queries went to different coordinators).
   *
   * <p>Note that schema agreement never succeeds in a mixed-version cluster (it would be
   * challenging because the way the schema version is computed varies across server versions); the
   * assumption is that schema updates are unlikely to happen during a rolling upgrade anyway.
   *
   * @return a future that completes with {@code true} if the nodes agree, or {@code false} if the
   *     timeout fired.
   * @see CoreDriverOption#CONTROL_CONNECTION_AGREEMENT_INTERVAL
   * @see CoreDriverOption#CONTROL_CONNECTION_AGREEMENT_TIMEOUT
   */
  CompletionStage<Boolean> checkSchemaAgreementAsync();

  /**
   * Convenience method to call {@link #checkSchemaAgreementAsync()} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default boolean checkSchemaAgreement() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(checkSchemaAgreementAsync());
  }

  /** Returns a context that provides access to all the policies used by this driver instance. */
  DriverContext getContext();

  /**
   * The keyspace that this session is currently connected to.
   *
   * <p>There are two ways that this can be set: before initializing the session (either with the
   * {@code session-keyspace} option in the configuration, or with {@link
   * CqlSessionBuilder#withKeyspace(CqlIdentifier)}); or at runtime, if the client issues a request
   * that changes the keyspace (such as a CQL {@code USE} query). Note that this second method is
   * inherently unsafe, since other requests expecting the old keyspace might be executing
   * concurrently. Therefore it is highly discouraged, aside from trivial cases (such as a
   * cqlsh-style program where requests are never concurrent).
   */
  CqlIdentifier getKeyspace();

  /**
   * The registry of driver metrics.
   *
   * <p>The driver is instrumented with DropWizard metrics, use this object to register metric
   * reporters.
   *
   * @see <a href="http://metrics.dropwizard.io/4.0.0/manual/core.html#reporters">Reporters
   *     (DropWizard Metrics manual)</a>
   */
  MetricRegistry getMetricRegistry();

  /**
   * Executes an arbitrary request.
   *
   * @param resultType the type of the result, which determines the internal request processor
   *     (built-in or custom) that will be used to handle the request.
   * @see Session
   */
  <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType);

  /**
   * Registers the provided schema change listener.
   *
   * <p>This is a no-op if the listener was registered already.
   */
  void register(SchemaChangeListener listener);

  /**
   * Unregisters the provided schema change listener.
   *
   * <p>This is a no-op if the listener was not registered.
   */
  void unregister(SchemaChangeListener listener);

  /**
   * Registers the provided node state listener.
   *
   * <p>This is a no-op if the listener was registered already.
   */
  void register(NodeStateListener listener);

  /**
   * Unregisters the provided node state listener.
   *
   * <p>This is a no-op if the listener was not registered.
   */
  void unregister(NodeStateListener listener);
}
