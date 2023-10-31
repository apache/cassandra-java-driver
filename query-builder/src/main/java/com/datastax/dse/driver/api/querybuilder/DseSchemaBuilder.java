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
package com.datastax.dse.driver.api.querybuilder;

import com.datastax.dse.driver.api.querybuilder.schema.AlterDseKeyspaceStart;
import com.datastax.dse.driver.api.querybuilder.schema.AlterDseTableStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseAggregateStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseKeyspaceStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseTableStart;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultAlterDseKeyspace;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultAlterDseTable;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultCreateDseAggregate;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultCreateDseFunction;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultCreateDseKeyspace;
import com.datastax.dse.driver.internal.querybuilder.schema.DefaultCreateDseTable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionStart;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An extension of {@link com.datastax.oss.driver.api.querybuilder.SchemaBuilder} for building
 * schema entities that have DSE specific functionality.
 */
public class DseSchemaBuilder extends SchemaBuilder {

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   */
  @Nonnull
  public static CreateDseAggregateStart createDseAggregate(@Nonnull CqlIdentifier aggregateId) {
    return new DefaultCreateDseAggregate(aggregateId);
  }

  /** Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name. */
  @Nonnull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable CqlIdentifier keyspaceId, @Nonnull CqlIdentifier aggregateId) {
    return new DefaultCreateDseAggregate(keyspaceId, aggregateId);
  }

  /**
   * Shortcut for {@link #createDseAggregate(CqlIdentifier)
   * createDseAggregate(CqlIdentifier.fromCql(aggregateName))}.
   */
  @Nonnull
  public static CreateDseAggregateStart createDseAggregate(@Nonnull String aggregateName) {
    return new DefaultCreateDseAggregate(CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * createDseAggregate(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(aggregateName))}.
   */
  @Nonnull
  public static CreateDseAggregateStart createDseAggregate(
      @Nullable String keyspaceName, @Nonnull String aggregateName) {
    return new DefaultCreateDseAggregate(
        keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName),
        CqlIdentifier.fromCql(aggregateName));
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name. This assumes the keyspace name
   * is already qualified for the Session or Statement.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(CqlIdentifier)}.
   */
  @Nonnull
  public static CreateAggregateStart createAggregate(@Nonnull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Starts a CREATE AGGREGATE query with the given aggregate name for the given keyspace name.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(CqlIdentifier, CqlIdentifier)}.
   */
  @Nonnull
  public static CreateAggregateStart createAggregate(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier aggregateName) {
    return SchemaBuilder.createAggregate(keyspace, aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier)
   * createAggregate(CqlIdentifier.fromCql(aggregateName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(String)}.
   */
  @Nonnull
  public static CreateAggregateStart createAggregate(@Nonnull String aggregateName) {
    return SchemaBuilder.createAggregate(aggregateName);
  }

  /**
   * Shortcut for {@link #createAggregate(CqlIdentifier, CqlIdentifier)
   * createAggregate(CqlIdentifier.fromCql(keyspace), CqlIdentifier.fromCql(aggregateName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code DETERMINISTIC} keyword, use {@link
   * #createDseAggregate(String, String)}.
   */
  @Nonnull
  public static CreateAggregateStart createAggregate(
      @Nullable String keyspace, @Nonnull String aggregateName) {
    return SchemaBuilder.createAggregate(keyspace, aggregateName);
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @Nonnull
  public static CreateDseFunctionStart createDseFunction(@Nonnull CqlIdentifier functionId) {
    return new DefaultCreateDseFunction(functionId);
  }

  /** Starts a CREATE FUNCTION query with the given function name for the given keyspace name. */
  @Nonnull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable CqlIdentifier keyspaceId, @Nonnull CqlIdentifier functionId) {
    return new DefaultCreateDseFunction(keyspaceId, functionId);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(functionName)}
   */
  @Nonnull
  public static CreateDseFunctionStart createDseFunction(@Nonnull String functionName) {
    return new DefaultCreateDseFunction(CqlIdentifier.fromCql(functionName));
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName)}
   */
  @Nonnull
  public static CreateDseFunctionStart createDseFunction(
      @Nullable String keyspaceName, @Nonnull String functionName) {
    return new DefaultCreateDseFunction(
        keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName),
        CqlIdentifier.fromCql(functionName));
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(CqlIdentifier)}.
   */
  @Nonnull
  public static CreateFunctionStart createFunction(@Nonnull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Starts a CREATE FUNCTION query with the given function name for the given keyspace name.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(CqlIdentifier,CqlIdentifier)}.
   */
  @Nonnull
  public static CreateFunctionStart createFunction(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier, CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspace, functionName)}
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(String)}.
   */
  @Nonnull
  public static CreateFunctionStart createFunction(@Nonnull String functionName) {
    return SchemaBuilder.createFunction(functionName);
  }

  /**
   * Shortcut for {@link #createFunction(CqlIdentifier)
   * createFunction(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(functionName)}.
   *
   * <p>Note that this method only covers open-source Cassandra syntax. If you want to use
   * DSE-specific features, such as the {@code MONOTONIC} or {@code DETERMINISTIC} keywords, use
   * {@link #createDseFunction(String, String)}.
   */
  @Nonnull
  public static CreateFunctionStart createFunction(
      @Nullable String keyspace, @Nonnull String functionName) {
    return SchemaBuilder.createFunction(keyspace, functionName);
  }

  /** Starts a CREATE KEYSPACE query. */
  @Nonnull
  public static CreateDseKeyspaceStart createDseKeyspace(@Nonnull CqlIdentifier keyspaceName) {
    return new DefaultCreateDseKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #createDseKeyspace(CqlIdentifier)
   * createKeyspace(CqlIdentifier.fromCql(keyspaceName))}
   */
  @Nonnull
  public static CreateDseKeyspaceStart createDseKeyspace(@Nonnull String keyspaceName) {
    return createDseKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** Starts an ALTER KEYSPACE query. */
  @Nonnull
  public static AlterDseKeyspaceStart alterDseKeyspace(@Nonnull CqlIdentifier keyspaceName) {
    return new DefaultAlterDseKeyspace(keyspaceName);
  }

  /**
   * Shortcut for {@link #alterDseKeyspace(CqlIdentifier)
   * alterKeyspace(CqlIdentifier.fromCql(keyspaceName)}.
   */
  @Nonnull
  public static AlterDseKeyspaceStart alterDseKeyspace(@Nonnull String keyspaceName) {
    return DseSchemaBuilder.alterDseKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Starts a CREATE TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @Nonnull
  public static CreateDseTableStart createDseTable(@Nonnull CqlIdentifier tableName) {
    return createDseTable(null, tableName);
  }

  /** Starts a CREATE TABLE query with the given table name for the given keyspace name. */
  @Nonnull
  public static CreateDseTableStart createDseTable(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier tableName) {
    return new DefaultCreateDseTable(keyspace, tableName);
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier)
   * createDseTable(CqlIdentifier.fromCql(tableName)}
   */
  @Nonnull
  public static CreateDseTableStart createDseTable(@Nonnull String tableName) {
    return createDseTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #createDseTable(CqlIdentifier,CqlIdentifier)
   * createDseTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  @Nonnull
  public static CreateDseTableStart createDseTable(
      @Nullable String keyspace, @Nonnull String tableName) {
    return createDseTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }

  /**
   * Starts an ALTER TABLE query with the given table name. This assumes the keyspace name is
   * already qualified for the Session or Statement.
   */
  @Nonnull
  public static AlterDseTableStart alterDseTable(@Nonnull CqlIdentifier tableName) {
    return new DefaultAlterDseTable(tableName);
  }

  /** Starts an ALTER TABLE query with the given table name for the given keyspace name. */
  @Nonnull
  public static AlterDseTableStart alterDseTable(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier tableName) {
    return new DefaultAlterDseTable(keyspace, tableName);
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier)
   * alterDseTable(CqlIdentifier.fromCql(tableName)}
   */
  @Nonnull
  public static AlterDseTableStart alterDseTable(@Nonnull String tableName) {
    return alterDseTable(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Shortcut for {@link #alterDseTable(CqlIdentifier,CqlIdentifier)
   * alterDseTable(CqlIdentifier.fromCql(keyspaceName),CqlIdentifier.fromCql(tableName)}
   */
  @Nonnull
  public static AlterDseTableStart alterDseTable(
      @Nullable String keyspace, @Nonnull String tableName) {
    return alterDseTable(
        keyspace == null ? null : CqlIdentifier.fromCql(keyspace),
        CqlIdentifier.fromCql(tableName));
  }
}
