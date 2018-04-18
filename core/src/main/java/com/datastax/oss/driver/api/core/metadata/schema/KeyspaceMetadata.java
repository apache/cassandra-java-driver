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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import java.util.Map;

/** A keyspace in the schema metadata. */
public interface KeyspaceMetadata extends Describable {
  CqlIdentifier getName();

  /** Whether durable writes are set on this keyspace. */
  boolean isDurableWrites();

  /** The replication options defined for this keyspace. */
  Map<String, String> getReplication();

  Map<CqlIdentifier, TableMetadata> getTables();

  default TableMetadata getTable(CqlIdentifier tableId) {
    return getTables().get(tableId);
  }

  /** Shortcut for {@link #getTable(CqlIdentifier) getTable(CqlIdentifier.fromCql(tableName))}. */
  default TableMetadata getTable(String tableName) {
    return getTable(CqlIdentifier.fromCql(tableName));
  }

  Map<CqlIdentifier, ViewMetadata> getViews();

  /** Gets the views based on a given table. */
  default Map<CqlIdentifier, ViewMetadata> getViewsOnTable(CqlIdentifier tableId) {
    ImmutableMap.Builder<CqlIdentifier, ViewMetadata> builder = ImmutableMap.builder();
    for (ViewMetadata view : getViews().values()) {
      if (view.getBaseTable().equals(tableId)) {
        builder.put(view.getName(), view);
      }
    }
    return builder.build();
  }

  default ViewMetadata getView(CqlIdentifier viewId) {
    return getViews().get(viewId);
  }

  /** Shortcut for {@link #getView(CqlIdentifier) getView(CqlIdentifier.fromCql(viewName))}. */
  default ViewMetadata getView(String viewName) {
    return getView(CqlIdentifier.fromCql(viewName));
  }

  Map<CqlIdentifier, UserDefinedType> getUserDefinedTypes();

  default UserDefinedType getUserDefinedType(CqlIdentifier typeId) {
    return getUserDefinedTypes().get(typeId);
  }

  /**
   * Shortcut for {@link #getUserDefinedType(CqlIdentifier)
   * getUserDefinedType(CqlIdentifier.fromCql(typeName))}.
   */
  default UserDefinedType getUserDefinedType(String typeName) {
    return getUserDefinedType(CqlIdentifier.fromCql(typeName));
  }

  Map<FunctionSignature, FunctionMetadata> getFunctions();

  default FunctionMetadata getFunction(FunctionSignature functionSignature) {
    return getFunctions().get(functionSignature);
  }

  default FunctionMetadata getFunction(
      CqlIdentifier functionId, Iterable<DataType> parameterTypes) {
    return getFunctions().get(new FunctionSignature(functionId, parameterTypes));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, Iterable)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   */
  default FunctionMetadata getFunction(String functionName, Iterable<DataType> parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  default FunctionMetadata getFunction(CqlIdentifier functionId, DataType... parameterTypes) {
    return getFunctions().get(new FunctionSignature(functionId, parameterTypes));
  }

  /**
   * Shortcut for {@link #getFunction(CqlIdentifier, DataType...)
   * getFunction(CqlIdentifier.fromCql(functionName), parameterTypes)}.
   */
  default FunctionMetadata getFunction(String functionName, DataType... parameterTypes) {
    return getFunction(CqlIdentifier.fromCql(functionName), parameterTypes);
  }

  Map<FunctionSignature, AggregateMetadata> getAggregates();

  default AggregateMetadata getAggregate(FunctionSignature aggregateSignature) {
    return getAggregates().get(aggregateSignature);
  }

  default AggregateMetadata getAggregate(
      CqlIdentifier aggregateId, Iterable<DataType> parameterTypes) {
    return getAggregates().get(new FunctionSignature(aggregateId, parameterTypes));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, Iterable)
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   */
  default AggregateMetadata getAggregate(String aggregateName, Iterable<DataType> parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  default AggregateMetadata getAggregate(CqlIdentifier aggregateId, DataType... parameterTypes) {
    return getAggregates().get(new FunctionSignature(aggregateId, parameterTypes));
  }

  /**
   * Shortcut for {@link #getAggregate(CqlIdentifier, DataType...)}
   * getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes)}.
   */
  default AggregateMetadata getAggregate(String aggregateName, DataType... parameterTypes) {
    return getAggregate(CqlIdentifier.fromCql(aggregateName), parameterTypes);
  }

  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder =
        new ScriptBuilder(pretty)
            .append("CREATE KEYSPACE ")
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
    return builder
        .append(" } AND durable_writes = ")
        .append(Boolean.toString(isDurableWrites()))
        .append(";")
        .build();
  }

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
}
