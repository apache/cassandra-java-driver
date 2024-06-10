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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** A keyspace in the schema metadata. */
public interface KeyspaceMetadata extends Describable {

  @NonNull
  CqlIdentifier getName();

  /** Whether durable writes are set on this keyspace. */
  boolean isDurableWrites();

  /** Whether this keyspace is virtual */
  boolean isVirtual();

  /** The replication options defined for this keyspace. */
  @NonNull
  Map<String, String> getReplication();

  @NonNull
  Map<CqlIdentifier, TableMetadata> getTables();

  @NonNull
  default Optional<TableMetadata> getTable(@NonNull CqlIdentifier tableId) {
    return Optional.ofNullable(getTables().get(tableId));
  }

  /** Shortcut for {@link #getTable(CqlIdentifier) getTable(CqlIdentifier.fromCql(tableName))}. */
  @NonNull
  default Optional<TableMetadata> getTable(@NonNull String tableName) {
    return getTable(CqlIdentifier.fromCql(tableName));
  }

  @NonNull
  Map<CqlIdentifier, ViewMetadata> getViews();

  /** Gets the views based on a given table. */
  @NonNull
  default Map<CqlIdentifier, ViewMetadata> getViewsOnTable(@NonNull CqlIdentifier tableId) {
    ImmutableMap.Builder<CqlIdentifier, ViewMetadata> builder = ImmutableMap.builder();
    for (ViewMetadata view : getViews().values()) {
      if (view.getBaseTable().equals(tableId)) {
        builder.put(view.getName(), view);
      }
    }
    return builder.build();
  }

  @NonNull
  default Optional<ViewMetadata> getView(@NonNull CqlIdentifier viewId) {
    return Optional.ofNullable(getViews().get(viewId));
  }

  /** Shortcut for {@link #getView(CqlIdentifier) getView(CqlIdentifier.fromCql(viewName))}. */
  @NonNull
  default Optional<ViewMetadata> getView(@NonNull String viewName) {
    return getView(CqlIdentifier.fromCql(viewName));
  }

  @NonNull
  Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes();

  @NonNull
  default Optional<UserDefinedType> getUserDefinedType(@NonNull CqlIdentifier typeId) {
    return Optional.ofNullable(getUserDefinedTypes().get(typeId));
  }

  /**
   * Shortcut for {@link #getUserDefinedType(CqlIdentifier)
   * getUserDefinedType(CqlIdentifier.fromCql(typeName))}.
   */
  @NonNull
  default Optional<UserDefinedType> getUserDefinedType(@NonNull String typeName) {
    return getUserDefinedType(CqlIdentifier.fromCql(typeName));
  }

  @NonNull
  Map<FunctionSignature, FunctionMetadata> getFunctions();

  @NonNull
  default Optional<FunctionMetadata> getFunction(@NonNull FunctionSignature functionSignature) {
    return Optional.ofNullable(getFunctions().get(functionSignature));
  }

  @NonNull
  default Optional<FunctionMetadata> getFunction(
      @NonNull CqlIdentifier functionId, @NonNull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, Iterable)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   */
  @NonNull
  default Optional<FunctionMetadata> getFunction(
      @NonNull String functionName, @NonNull Iterable<DataType> parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  /**
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @NonNull
  default Optional<FunctionMetadata> getFunction(
      @NonNull CqlIdentifier functionId, @NonNull DataType... parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, DataType...)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   *
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @NonNull
  default Optional<FunctionMetadata> getFunction(
      @NonNull String functionName, @NonNull DataType... parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  @NonNull
  Map<FunctionSignature, AggregateMetadata> getAggregates();

  @NonNull
  default Optional<AggregateMetadata> getAggregate(@NonNull FunctionSignature aggregateSignature) {
    return Optional.ofNullable(getAggregates().get(aggregateSignature));
  }

  @NonNull
  default Optional<AggregateMetadata> getAggregate(
      @NonNull CqlIdentifier aggregateId, @NonNull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, Iterable)
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   */
  @NonNull
  default Optional<AggregateMetadata> getAggregate(
      @NonNull String aggregateName, @NonNull Iterable<DataType> parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  /**
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @NonNull
  default Optional<AggregateMetadata> getAggregate(
      @NonNull CqlIdentifier aggregateId, @NonNull DataType... parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, DataType...)}
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   *
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @NonNull
  default Optional<AggregateMetadata> getAggregate(
      @NonNull String aggregateName, @NonNull DataType... parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    if (isVirtual()) {
      builder.append("/* VIRTUAL ");
    } else {
      builder.append("CREATE ");
    }
    builder
        .append("KEYSPACE ")
        .append(getName())
        .append(" WITH replication = { 'class' : '")
        .append(getReplication().get("class"))
        .append("'");
    for (Map.Entry<String, String> entry : getReplication().entrySet()) {
      if (!entry.getKey().equals("class")) {
        builder
            .append(", '")
            .append(entry.getKey())
            .append("': '")
            .append(entry.getValue())
            .append("'");
      }
    }
    builder
        .append(" } AND durable_writes = ")
        .append(Boolean.toString(isDurableWrites()))
        .append(";");
    if (isVirtual()) {
      builder.append(" */");
    }
    return builder.build();
  }

  @NonNull
  @Override
  default String describeWithChildren(boolean pretty) {
    String createKeyspace = describe(pretty);
    ScriptBuilder builder = new ScriptBuilder(pretty).append(createKeyspace);

    for (Describable element :
        Iterables.concat(
            getUserDefinedTypes().values(),
            getTables().values(),
            getViews().values(),
            getFunctions().values(),
            getAggregates().values())) {
      builder.forceNewLine(2).append(element.describeWithChildren(pretty));
    }

    return builder.build();
  }

  default boolean shallowEquals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof KeyspaceMetadata) {
      KeyspaceMetadata that = (KeyspaceMetadata) other;
      return Objects.equals(this.getName(), that.getName())
          && this.isDurableWrites() == that.isDurableWrites()
          && Objects.equals(this.getReplication(), that.getReplication());
    } else {
      return false;
    }
  }
}
