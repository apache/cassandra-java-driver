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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.concurrent.CompletionStage;

/** An instance of the driver, that connects to a Cassandra cluster. */
public interface Cluster extends AsyncAutoCloseable {
  /** Returns a builder to create a new instance of the default implementation. */
  static ClusterBuilder builder() {
    return new ClusterBuilder();
  }

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

  /** Returns a context that provides access to all the policies used by this driver instance. */
  DriverContext getContext();

  /** Creates a new session to execute requests against a given keyspace. */
  CompletionStage<CqlSession> connectAsync(CqlIdentifier keyspace);

  /**
   * Creates a new session not tied to any keyspace.
   *
   * <p>This is equivalent to {@code this.connectAsync(null)}.
   */
  default CompletionStage<CqlSession> connectAsync() {
    return connectAsync(null);
  }

  /**
   * Convenience method to call {@link #connectAsync(CqlIdentifier)} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default CqlSession connect(CqlIdentifier keyspace) {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(connectAsync(keyspace));
  }

  /**
   * Convenience method to call {@link #connectAsync()} and block for the result.
   *
   * <p>This must not be called on a driver thread.
   */
  default CqlSession connect() {
    return connect(null);
  }
}
