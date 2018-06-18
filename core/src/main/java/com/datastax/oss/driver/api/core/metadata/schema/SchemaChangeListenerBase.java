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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.type.UserDefinedType;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Convenience class for listener implementations that that don't need to override all methods (all
 * methods in this class are empty).
 */
public class SchemaChangeListenerBase implements SchemaChangeListener {

  @Override
  public void onKeyspaceCreated(@NonNull KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceDropped(@NonNull KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceUpdated(
      @NonNull KeyspaceMetadata current, @NonNull KeyspaceMetadata previous) {
    // nothing to do
  }

  @Override
  public void onTableCreated(@NonNull TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableDropped(@NonNull TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableUpdated(@NonNull TableMetadata current, @NonNull TableMetadata previous) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeCreated(@NonNull UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeDropped(@NonNull UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeUpdated(
      @NonNull UserDefinedType current, @NonNull UserDefinedType previous) {
    // nothing to do
  }

  @Override
  public void onFunctionCreated(@NonNull FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionDropped(@NonNull FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionUpdated(
      @NonNull FunctionMetadata current, @NonNull FunctionMetadata previous) {
    // nothing to do
  }

  @Override
  public void onAggregateCreated(@NonNull AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateDropped(@NonNull AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateUpdated(
      @NonNull AggregateMetadata current, @NonNull AggregateMetadata previous) {
    // nothing to do
  }

  @Override
  public void onViewCreated(@NonNull ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewDropped(@NonNull ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewUpdated(@NonNull ViewMetadata current, @NonNull ViewMetadata previous) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
