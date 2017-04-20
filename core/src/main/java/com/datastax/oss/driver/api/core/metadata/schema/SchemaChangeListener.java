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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Tracks schema changes.
 *
 * <p>An implementation of this interface can be registered with {@link
 * Cluster#register(SchemaChangeListener)}.
 *
 * <p>Note that the methods defined by this interface will be executed by internal driver threads,
 * and are therefore expected to have short execution times. If you need to perform long
 * computations or blocking calls in response to schema change events, it is strongly recommended to
 * schedule them asynchronously on a separate thread provided by your application code.
 */
public interface SchemaChangeListener {

  void onKeyspaceCreated(KeyspaceMetadata keyspace);

  void onKeyspaceDropped(KeyspaceMetadata keyspace);

  void onKeyspaceUpdated(KeyspaceMetadata current, KeyspaceMetadata previous);

  void onTableCreated(TableMetadata table);

  void onTableDropped(TableMetadata table);

  void onTableUpdated(TableMetadata current, TableMetadata previous);

  void onUserDefinedTypeCreated(UserDefinedType type);

  void onUserDefinedTypeDropped(UserDefinedType type);

  void onUserDefinedTypeUpdated(UserDefinedType current, UserDefinedType previous);

  void onFunctionCreated(FunctionMetadata function);

  void onFunctionDropped(FunctionMetadata function);

  void onFunctionUpdated(FunctionMetadata current, FunctionMetadata previous);

  void onAggregateCreated(AggregateMetadata aggregate);

  void onAggregateDropped(AggregateMetadata aggregate);

  void onAggregateUpdated(AggregateMetadata current, AggregateMetadata previous);

  void onViewCreated(ViewMetadata view);

  void onViewDropped(ViewMetadata view);

  void onViewUpdated(ViewMetadata current, ViewMetadata previous);
  /** Invoked when the listener is registered with a cluster. */
  void onRegister(Cluster cluster);

  /**
   * Invoked when the listener is unregistered from a cluster, or at cluster shutdown, whichever
   * comes first.
   */
  void onUnregister(Cluster cluster);
}
