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

/**
 * Convenience class for listener implementations that that don't need to override all methods (all
 * methods in this class are empty).
 */
public class SchemaChangeListenerBase implements SchemaChangeListener {

  @Override
  public void onKeyspaceCreated(KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceDropped(KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceUpdated(KeyspaceMetadata current, KeyspaceMetadata previous) {
    // nothing to do
  }

  @Override
  public void onTableCreated(TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableDropped(TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableUpdated(TableMetadata current, TableMetadata previous) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeCreated(UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeDropped(UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeUpdated(UserDefinedType current, UserDefinedType previous) {
    // nothing to do
  }

  @Override
  public void onFunctionCreated(FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionDropped(FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionUpdated(FunctionMetadata current, FunctionMetadata previous) {
    // nothing to do
  }

  @Override
  public void onAggregateCreated(AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateDropped(AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateUpdated(AggregateMetadata current, AggregateMetadata previous) {
    // nothing to do
  }

  @Override
  public void onViewCreated(ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewDropped(ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewUpdated(ViewMetadata current, ViewMetadata previous) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
