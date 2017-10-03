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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.ResourceBundle;
import java.util.concurrent.CompletionStage;

/**
 * An instance of the driver, that connects to a Cassandra cluster.
 *
 * @param <SessionT> the type of session returned by this cluster. By default, this is {@link
 *     CqlSession}.
 */
public interface Cluster<SessionT extends Session> extends AsyncAutoCloseable {

  /** Returns a builder to create a new instance of the default implementation. */
  static DefaultClusterBuilder builder() {
    return new DefaultClusterBuilder();
  }

  /**
   * The current version of the driver.
   *
   * <p>This is intended for products that wrap or extend the driver, as a way to check
   * compatibility if end-users override the driver version in their application.
   */
  static String getDriverVersion() {
    // Note: getBundle caches its result
    return ResourceBundle.getBundle("com.datastax.oss.driver.Driver").getString("driver.version");
  }

  /**
   * The unique name identifying this cluster.
   *
   * @see CoreDriverOption#CLUSTER_NAME
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

  /** Returns a context that provides access to all the policies used by this driver instance. */
  DriverContext getContext();

  /** Creates a new session to execute requests against a given keyspace. */
  CompletionStage<SessionT> connectAsync(CqlIdentifier keyspace);

  /**
   * Creates a new session not tied to any keyspace.
   *
   * <p>This is equivalent to {@code this.connectAsync(null)}.
   */
  default CompletionStage<SessionT> connectAsync() {
    return connectAsync(null);
  }

  /**
   * Convenience method to call {@link #connectAsync(CqlIdentifier)} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default SessionT connect(CqlIdentifier keyspace) {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(connectAsync(keyspace));
  }

  /**
   * Convenience method to call {@link #connectAsync()} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default SessionT connect() {
    return connect(null);
  }

  /**
   * Registers the provided schema change listener.
   *
   * <p>This is a no-op if the listener was registered already.
   */
  Cluster register(SchemaChangeListener listener);

  /**
   * Unregisters the provided schema change listener.
   *
   * <p>This is a no-op if the listener was not registered.
   */
  Cluster unregister(SchemaChangeListener listener);

  /**
   * Registers the provided node state listener.
   *
   * <p>This is a no-op if the listener was registered already.
   */
  Cluster register(NodeStateListener listener);

  /**
   * Unregisters the provided node state listener.
   *
   * <p>This is a no-op if the listener was not registered.
   */
  Cluster unregister(NodeStateListener listener);
}
