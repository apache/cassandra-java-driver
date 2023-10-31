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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import javax.annotation.Nonnull;

/**
 * Tracks schema changes.
 *
 * <p>Implementations of this interface can be registered either via the configuration (see {@code
 * reference.conf} in the manual or core driver JAR), or programmatically via {@link
 * SessionBuilder#addSchemaChangeListener(SchemaChangeListener)}.
 *
 * <p>Note that the methods defined by this interface will be executed by internal driver threads,
 * and are therefore expected to have short execution times. If you need to perform long
 * computations or blocking calls in response to schema change events, it is strongly recommended to
 * schedule them asynchronously on a separate thread provided by your application code.
 *
 * <p>If you implement this interface but don't need to implement all the methods, extend {@link
 * SchemaChangeListenerBase}.
 */
public interface SchemaChangeListener extends AutoCloseable {

  void onKeyspaceCreated(@Nonnull KeyspaceMetadata keyspace);

  void onKeyspaceDropped(@Nonnull KeyspaceMetadata keyspace);

  void onKeyspaceUpdated(@Nonnull KeyspaceMetadata current, @Nonnull KeyspaceMetadata previous);

  void onTableCreated(@Nonnull TableMetadata table);

  void onTableDropped(@Nonnull TableMetadata table);

  void onTableUpdated(@Nonnull TableMetadata current, @Nonnull TableMetadata previous);

  void onUserDefinedTypeCreated(@Nonnull UserDefinedType type);

  void onUserDefinedTypeDropped(@Nonnull UserDefinedType type);

  void onUserDefinedTypeUpdated(
      @Nonnull UserDefinedType current, @Nonnull UserDefinedType previous);

  void onFunctionCreated(@Nonnull FunctionMetadata function);

  void onFunctionDropped(@Nonnull FunctionMetadata function);

  void onFunctionUpdated(@Nonnull FunctionMetadata current, @Nonnull FunctionMetadata previous);

  void onAggregateCreated(@Nonnull AggregateMetadata aggregate);

  void onAggregateDropped(@Nonnull AggregateMetadata aggregate);

  void onAggregateUpdated(@Nonnull AggregateMetadata current, @Nonnull AggregateMetadata previous);

  void onViewCreated(@Nonnull ViewMetadata view);

  void onViewDropped(@Nonnull ViewMetadata view);

  void onViewUpdated(@Nonnull ViewMetadata current, @Nonnull ViewMetadata previous);

  /**
   * Invoked when the session is ready to process user requests.
   *
   * <p>This corresponds to the moment when {@link SessionBuilder#build()} returns, or the future
   * returned by {@link SessionBuilder#buildAsync()} completes. If the session initialization fails,
   * this method will not get called.
   *
   * <p>Listener methods are invoked from different threads; if you store the session in a field,
   * make it at least volatile to guarantee proper publication.
   *
   * <p>This method is guaranteed to be the first one invoked on this object.
   *
   * <p>The default implementation is empty.
   */
  default void onSessionReady(@Nonnull Session session) {}
}
