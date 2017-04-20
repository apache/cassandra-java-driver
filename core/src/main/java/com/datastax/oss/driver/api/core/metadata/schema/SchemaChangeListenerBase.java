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
 * Convenience schema change listener implementation that defines all methods as no-ops.
 *
 * <p>Implementors that are only interested in a subset of events can extend this class and override
 * the relevant methods.
 */
public class SchemaChangeListenerBase implements SchemaChangeListener {

  @Override
  public void onKeyspaceCreated(KeyspaceMetadata keyspace) {}

  @Override
  public void onKeyspaceDropped(KeyspaceMetadata keyspace) {}

  @Override
  public void onKeyspaceUpdated(KeyspaceMetadata current, KeyspaceMetadata previous) {}

  @Override
  public void onTableCreated(TableMetadata table) {}

  @Override
  public void onTableDropped(TableMetadata table) {}

  @Override
  public void onTableUpdated(TableMetadata current, TableMetadata previous) {}

  @Override
  public void onUserDefinedTypeCreated(UserDefinedType type) {}

  @Override
  public void onUserDefinedTypeDropped(UserDefinedType type) {}

  @Override
  public void onUserDefinedTypeUpdated(UserDefinedType current, UserDefinedType previous) {}

  @Override
  public void onFunctionCreated(FunctionMetadata function) {}

  @Override
  public void onFunctionDropped(FunctionMetadata function) {}

  @Override
  public void onFunctionUpdated(FunctionMetadata current, FunctionMetadata previous) {}

  @Override
  public void onAggregateCreated(AggregateMetadata aggregate) {}

  @Override
  public void onAggregateDropped(AggregateMetadata aggregate) {}

  @Override
  public void onAggregateUpdated(AggregateMetadata current, AggregateMetadata previous) {}

  @Override
  public void onViewCreated(ViewMetadata view) {}

  @Override
  public void onViewDropped(ViewMetadata view) {}

  @Override
  public void onViewUpdated(ViewMetadata current, ViewMetadata previous) {}

  @Override
  public void onRegister(Cluster cluster) {}

  @Override
  public void onUnregister(Cluster cluster) {}
}
