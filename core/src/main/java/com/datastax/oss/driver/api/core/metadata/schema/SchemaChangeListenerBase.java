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

import com.datastax.oss.driver.api.core.type.UserDefinedType;
import javax.annotation.Nonnull;

/**
 * Convenience class for listener implementations that that don't need to override all methods (all
 * methods in this class are empty).
 */
public class SchemaChangeListenerBase implements SchemaChangeListener {

  @Override
  public void onKeyspaceCreated(@Nonnull KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceDropped(@Nonnull KeyspaceMetadata keyspace) {
    // nothing to do
  }

  @Override
  public void onKeyspaceUpdated(
      @Nonnull KeyspaceMetadata current, @Nonnull KeyspaceMetadata previous) {
    // nothing to do
  }

  @Override
  public void onTableCreated(@Nonnull TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableDropped(@Nonnull TableMetadata table) {
    // nothing to do
  }

  @Override
  public void onTableUpdated(@Nonnull TableMetadata current, @Nonnull TableMetadata previous) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeCreated(@Nonnull UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeDropped(@Nonnull UserDefinedType type) {
    // nothing to do
  }

  @Override
  public void onUserDefinedTypeUpdated(
      @Nonnull UserDefinedType current, @Nonnull UserDefinedType previous) {
    // nothing to do
  }

  @Override
  public void onFunctionCreated(@Nonnull FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionDropped(@Nonnull FunctionMetadata function) {
    // nothing to do
  }

  @Override
  public void onFunctionUpdated(
      @Nonnull FunctionMetadata current, @Nonnull FunctionMetadata previous) {
    // nothing to do
  }

  @Override
  public void onAggregateCreated(@Nonnull AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateDropped(@Nonnull AggregateMetadata aggregate) {
    // nothing to do
  }

  @Override
  public void onAggregateUpdated(
      @Nonnull AggregateMetadata current, @Nonnull AggregateMetadata previous) {
    // nothing to do
  }

  @Override
  public void onViewCreated(@Nonnull ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewDropped(@Nonnull ViewMetadata view) {
    // nothing to do
  }

  @Override
  public void onViewUpdated(@Nonnull ViewMetadata current, @Nonnull ViewMetadata previous) {
    // nothing to do
  }

  @Override
  public void close() throws Exception {
    // nothing to do
  }
}
