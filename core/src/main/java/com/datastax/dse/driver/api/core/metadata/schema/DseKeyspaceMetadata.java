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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Optional;

/**
 * Specialized keyspace metadata for DSE.
 *
 * <p>It has the following differences with {@link KeyspaceMetadata}:
 *
 * <ul>
 *   <li>new method {@link #getGraphEngine()};
 *   <li>all sub-elements are specialized for DSE (e.g. {@link #getTables()} returns {@link
 *       DseTableMetadata} instances).
 * </ul>
 */
public interface DseKeyspaceMetadata extends KeyspaceMetadata {

  @NonNull
  @Override
  Map<CqlIdentifier, TableMetadata> getTables();

  @NonNull
  @Override
  default Optional<TableMetadata> getTable(@NonNull CqlIdentifier tableId) {
    return Optional.ofNullable(getTables().get(tableId));
  }

  @NonNull
  @Override
  default Optional<TableMetadata> getTable(@NonNull String tableName) {
    return getTable(CqlIdentifier.fromCql(tableName));
  }

  @NonNull
  @Override
  Map<CqlIdentifier, ViewMetadata> getViews();

  @NonNull
  @Override
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
  @Override
  default Optional<ViewMetadata> getView(@NonNull CqlIdentifier viewId) {
    return Optional.ofNullable(getViews().get(viewId));
  }

  @NonNull
  @Override
  default Optional<ViewMetadata> getView(@NonNull String viewName) {
    return getView(CqlIdentifier.fromCql(viewName));
  }

  @NonNull
  @Override
  Map<FunctionSignature, FunctionMetadata> getFunctions();

  @NonNull
  @Override
  default Optional<FunctionMetadata> getFunction(@NonNull FunctionSignature functionSignature) {
    return Optional.ofNullable(getFunctions().get(functionSignature));
  }

  @NonNull
  @Override
  default Optional<FunctionMetadata> getFunction(
      @NonNull CqlIdentifier functionId, @NonNull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  @NonNull
  @Override
  default Optional<FunctionMetadata> getFunction(
      @NonNull String functionName, @NonNull Iterable<DataType> parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  @NonNull
  @Override
  default Optional<FunctionMetadata> getFunction(
      @NonNull CqlIdentifier functionId, @NonNull DataType... parameterTypes) {
    return Optional.ofNullable(
        getFunctions().get(new FunctionSignature(functionId, parameterTypes)));
  }

  @NonNull
  @Override
  default Optional<FunctionMetadata> getFunction(
      @NonNull String functionName, @NonNull DataType... parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  @NonNull
  @Override
  Map<FunctionSignature, AggregateMetadata> getAggregates();

  @NonNull
  @Override
  default Optional<AggregateMetadata> getAggregate(@NonNull FunctionSignature aggregateSignature) {
    return Optional.ofNullable(getAggregates().get(aggregateSignature));
  }

  @NonNull
  @Override
  default Optional<AggregateMetadata> getAggregate(
      @NonNull CqlIdentifier aggregateId, @NonNull Iterable<DataType> parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  @NonNull
  @Override
  default Optional<AggregateMetadata> getAggregate(
      @NonNull String aggregateName, @NonNull Iterable<DataType> parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  @NonNull
  @Override
  default Optional<AggregateMetadata> getAggregate(
      @NonNull CqlIdentifier aggregateId, @NonNull DataType... parameterTypes) {
    return Optional.ofNullable(
        getAggregates().get(new FunctionSignature(aggregateId, parameterTypes)));
  }

  @NonNull
  @Override
  default Optional<AggregateMetadata> getAggregate(
      @NonNull String aggregateName, @NonNull DataType... parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  /** The graph engine that will be used to interpret this keyspace. */
  @NonNull
  Optional<String> getGraphEngine();

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
    builder.append(" } AND durable_writes = ").append(Boolean.toString(isDurableWrites()));
    getGraphEngine()
        .ifPresent(
            graphEngine -> builder.append(" AND graph_engine ='").append(graphEngine).append("'"));
    builder.append(";");
    if (isVirtual()) {
      builder.append(" */");
    }
    return builder.build();
  }
}
