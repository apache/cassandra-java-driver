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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

/** A keyspace in the schema metadata. */
public interface KeyspaceMetadata extends Describable {

  @Nonnull
  CqlIdentifier getName();

  /** Whether durable writes are set on this keyspace. */
  boolean isDurableWrites();

  /** Whether this keyspace is virtual */
  boolean isVirtual();

  /** The replication options defined for this keyspace. */
  @Nonnull
  Map<String, String> getReplication();

  @Nonnull
  Map<CqlIdentifier, TableMetadata> getTables();

  @Nonnull
  default Optional<TableMetadata> getTable(@Nonnull CqlIdentifier tableId) {
    return Optional.ofNullable(getTables().get(tableId));
  }

  /** Shortcut for {@link #getTable(CqlIdentifier) getTable(CqlIdentifier.fromCql(tableName))}. */
  @Nonnull
  default Optional<TableMetadata> getTable(@Nonnull String tableName) {
    return getTable(CqlIdentifier.fromCql(tableName));
  }

  @Nonnull
  Map<CqlIdentifier, ViewMetadata> getViews();

  /** Gets the views based on a given table. */
  @Nonnull
  default Map<CqlIdentifier, ViewMetadata> getViewsOnTable(@Nonnull CqlIdentifier tableId) {
    ImmutableMap.Builder<CqlIdentifier, ViewMetadata> builder = ImmutableMap.builder();
    for (ViewMetadata view : getViews().values()) {
      if (view.getBaseTable().equals(tableId)) {
        builder.put(view.getName(), view);
      }
    }
    return builder.build();
  }

  @Nonnull
  default Optional<ViewMetadata> getView(@Nonnull CqlIdentifier viewId) {
    return Optional.ofNullable(getViews().get(viewId));
  }

  /** Shortcut for {@link #getView(CqlIdentifier) getView(CqlIdentifier.fromCql(viewName))}. */
  @Nonnull
  default Optional<ViewMetadata> getView(@Nonnull String viewName) {
    return getView(CqlIdentifier.fromCql(viewName));
  }

  @Nonnull
  Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes();

  @Nonnull
  default Optional<UserDefinedType> getUserDefinedType(@Nonnull CqlIdentifier typeId) {
    return Optional.ofNullable(getUserDefinedTypes().get(typeId));
  }

  /**
   * Shortcut for {@link #getUserDefinedType(CqlIdentifier)
   * getUserDefinedType(CqlIdentifier.fromCql(typeName))}.
   */
  @Nonnull
  default Optional<UserDefinedType> getUserDefinedType(@Nonnull String typeName) {
    return getUserDefinedType(CqlIdentifier.fromCql(typeName));
  }

  @Nonnull
  Map<FunctionSignature, FunctionMetadata> getFunctions();

  @Nonnull
  default Optional<FunctionMetadata> getFunction(@Nonnull FunctionSignature functionSignature) {
    return Optional.ofNullable(getFunctions().get(functionSignature));
  }

  @Nonnull
  default Optional<FunctionMetadata> getFunction(
      @Nonnull CqlIdentifier functionId, @Nonnull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, Iterable)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   */
  @Nonnull
  default Optional<FunctionMetadata> getFunction(
      @Nonnull String functionName, @Nonnull Iterable<DataType> parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  /**
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @Nonnull
  default Optional<FunctionMetadata> getFunction(
      @Nonnull CqlIdentifier functionId, @Nonnull DataType... parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, DataType...)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   *
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @Nonnull
  default Optional<FunctionMetadata> getFunction(
      @Nonnull String functionName, @Nonnull DataType... parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  @Nonnull
  Map<FunctionSignature, AggregateMetadata> getAggregates();

  @Nonnull
  default Optional<AggregateMetadata> getAggregate(@Nonnull FunctionSignature aggregateSignature) {
    return Optional.ofNullable(getAggregates().get(aggregateSignature));
  }

  @Nonnull
  default Optional<AggregateMetadata> getAggregate(
      @Nonnull CqlIdentifier aggregateId, @Nonnull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, Iterable)
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   */
  @Nonnull
  default Optional<AggregateMetadata> getAggregate(
      @Nonnull String aggregateName, @Nonnull Iterable<DataType> parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  /**
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @Nonnull
  default Optional<AggregateMetadata> getAggregate(
      @Nonnull CqlIdentifier aggregateId, @Nonnull DataType... parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, DataType...)}
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   *
   * @param parameterTypes neither the individual types, nor the vararg array itself, can be null.
   */
  @Nonnull
  default Optional<AggregateMetadata> getAggregate(
      @Nonnull String aggregateName, @Nonnull DataType... parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  @Nonnull
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

  @Nonnull
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
