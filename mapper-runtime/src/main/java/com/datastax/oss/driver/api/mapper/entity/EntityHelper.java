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
package com.datastax.oss.driver.api.mapper.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A set of utility methods related to a particular mapped entity.
 *
 * <p>The mapper processor generates an implementation of this interface for each {@link
 * Entity}-annotated class. It is used internally by other mapper components, and can also be
 * injected in custom query providers.
 */
public interface EntityHelper<EntityT> {

  /**
   * Sets the properties of an entity instance into a target data structure.
   *
   * @deprecated Use {@link #set(Object, SettableByName, NullSavingStrategy, boolean)} instead.
   */
  @NonNull
  @Deprecated
  <SettableT extends SettableByName<SettableT>> SettableT set(
      @NonNull EntityT entity,
      @NonNull SettableT target,
      @NonNull NullSavingStrategy nullSavingStrategy);

  /**
   * Sets the properties of an entity instance into a target data structure.
   *
   * <p>The generated code will attempt to write all entity properties in the target data structure.
   * For example:
   *
   * <pre>{@code
   * target = target.set("id", entity.getId(), UUID.class);
   * target = target.set("name", entity.getName(), String.class);
   * ...
   * }</pre>
   *
   * The column names are inferred from the naming strategy for this entity.
   *
   * <p>The target will typically be one of the built-in driver subtypes: {@link BoundStatement},
   * {@link BoundStatementBuilder} or {@link UdtValue}. Note that the default {@link BoundStatement}
   * implementation is immutable, therefore this argument won't be modified in-place: you need to
   * use the return value to get the resulting structure.
   *
   * <p>If {@code lenient} is {@code true}, the mapper will operate on a best-effort basis and
   * attempt to write all entity properties that have a matching column in the target, leaving
   * unmatched properties untouched. Beware that this may result in a partially-populated target.
   *
   * <p>If {@code lenient} is {@code false}, then the target must contain a matching column for
   * every property in the entity definition, <em>except computed ones</em>. If such a column is not
   * found, an {@link IllegalArgumentException} will be thrown.
   *
   * @param entity the entity that the values will be read from.
   * @param target the data structure to fill.
   * @param lenient whether to tolerate incomplete targets.
   * @return the data structure resulting from the assignments. This is useful for immutable target
   *     implementations (see above), otherwise it will be the same as {@code target}.
   * @throws IllegalArgumentException if lenient is false and the target does not contain matching
   *     columns for every entity property.
   */
  @NonNull
  default <SettableT extends SettableByName<SettableT>> SettableT set(
      @NonNull EntityT entity,
      @NonNull SettableT target,
      @NonNull NullSavingStrategy nullSavingStrategy,
      boolean lenient) {
    return set(entity, target, nullSavingStrategy);
  }

  /**
   * Gets values from a data structure to fill an entity instance.
   *
   * @deprecated Use {@link #get(GettableByName, boolean)} instead.
   */
  @NonNull
  @Deprecated
  EntityT get(@NonNull GettableByName source);

  /**
   * Gets values from a data structure to fill an entity instance.
   *
   * <p>The generated code will attempt to read all entity properties from the source data
   * structure. For example:
   *
   * <pre>{@code
   * User returnValue = new User();
   * returnValue.setId(source.get("id", UUID.class));
   * returnValue.setName(source.get("name", String.class));
   * ...
   * }</pre>
   *
   * The column names are inferred from the naming strategy for this entity.
   *
   * <p>The source will typically be one of the built-in driver subtypes: {@link Row} or {@link
   * UdtValue} ({@link BoundStatement} and {@link BoundStatementBuilder} are also possible, although
   * it's less likely that data would be read back from them in this manner).
   *
   * <p>If {@code lenient} is {@code true}, the mapper will operate on a best-effort basis and
   * attempt to read all entity properties that have a matching column in the source, leaving
   * unmatched properties untouched. Beware that this may result in a partially-populated entity
   * instance.
   *
   * <p>If {@code lenient} is {@code false}, then the source must contain a matching column for
   * every property in the entity definition, <em>including computed ones</em>. If such a column is
   * not found, an {@link IllegalArgumentException} will be thrown.
   *
   * @param source the data structure to read from.
   * @param lenient whether to tolerate incomplete sources.
   * @return the resulting entity.
   * @throws IllegalArgumentException if lenient is false and the source does not contain matching
   *     columns for every entity property.
   */
  @NonNull
  default EntityT get(@NonNull GettableByName source, boolean lenient) {
    return get(source);
  }

  /**
   * Builds an insert query for this entity.
   *
   * <p>The returned query is roughly the equivalent of:
   *
   * <pre>{@code
   * QueryBuilder.insertInto(keyspaceId, tableId)
   *     .value("id", QueryBuilder.bindMarker("id"))
   *     .value("name", QueryBuilder.bindMarker("name"))
   *     ...
   * }</pre>
   *
   * All mapped properties of the entity are included as bindable values (the bind markers have the
   * same names as the columns).
   *
   * <p>The column names are inferred from the naming strategy for this entity.
   *
   * <p>The keyspace and table identifiers are those of the DAO that this helper was obtained from;
   * if the DAO was built without a specific keyspace and table, the query doesn't specify a
   * keyspace, and the table name is inferred from the naming strategy.
   */
  @NonNull
  RegularInsert insert();

  /**
   * Builds the beginning of a Update query to update an entity.
   *
   * <p>This is the same as {@link #updateByPrimaryKey()} ()}, but without the {@code WHERE} clause.
   * This would typically not be executed as-is, but instead completed with a custom {@code WHERE}
   * clause (either added with the query builder DSL, or concatenated to the built query).
   */
  @NonNull
  Update updateStart();

  /**
   * Builds a Update query to update an instance of the entity by primary key (partition key +
   * clustering columns).
   *
   * <p>The returned query is roughly the equivalent of:
   *
   * <pre>{@code
   * QueryBuilder.update(keyspaceId, tableId)
   *     .setColumn("description", QueryBuilder.bindMarker("description"))
   *     ... // (other non-PK columns)
   *     .where(Relation.column("id").isEqualTo(QueryBuilder.bindMarker("id"))
   *     ... // (other PK columns)
   * }</pre>
   *
   * All non-PK properties of the entity are set, with bind markers that have the same names as the
   * columns.
   *
   * <p>All components of the primary key are listed in the {@code WHERE} clause as bindable values
   * (the bind markers have the same names as the columns). They are listed in the natural order,
   * i.e. partition key columns first, followed by clustering columns (in the order defined by the
   * {@link PartitionKey} and {@link ClusteringColumn} annotations on the entity class).
   *
   * <p>The keyspace and table identifiers are those of the DAO that this helper was obtained from;
   * if the DAO was built without a specific keyspace and table, the query doesn't specify a
   * keyspace, and the table name is inferred from the naming strategy.
   */
  @NonNull
  Update updateByPrimaryKey();

  /**
   * Builds a select query to fetch an instance of the entity by primary key (partition key +
   * clustering columns).
   *
   * <p>The returned query is roughly the equivalent of:
   *
   * <pre>{@code
   * QueryBuilder.selectFrom(keyspaceId, tableId)
   *     .column("id")
   *     .column("name")
   *     .whereColumn("id").isEqualTo(QueryBuilder.bindMarker("id"));
   *   ...
   * }</pre>
   *
   * All mapped properties of the entity are included in the result set.
   *
   * <p>All components of the primary key are listed in the {@code WHERE} clause as bindable values
   * (the bind markers have the same names as the columns). They are listed in the natural order,
   * i.e. partition key columns first, followed by clustering columns (in the order defined by the
   * {@link PartitionKey} and {@link ClusteringColumn} annotations on the entity class).
   *
   * <p>The keyspace and table identifiers are those of the DAO that this helper was obtained from;
   * if the DAO was built without a specific keyspace and table, the query doesn't specify a
   * keyspace, and the table name is inferred from the naming strategy.
   */
  @NonNull
  Select selectByPrimaryKey();

  /**
   * Builds the beginning of a select query to fetch one or more instances of the entity.
   *
   * <p>This is the same as {@link #selectByPrimaryKey()}, but without the {@code WHERE} clause.
   * This would typically not be executed as-is, but instead completed with a custom {@code WHERE}
   * clause (either added with the query builder DSL, or concatenated to the built query).
   */
  @NonNull
  Select selectStart();

  /**
   * Builds a delete query to delete an instance of the entity by primary key (partition key +
   * clustering columns).
   *
   * <p>The returned query is roughly the equivalent of:
   *
   * <pre>{@code
   * Delete delete = QueryBuilder.deleteFrom(keyspaceId, tableId)
   *     .whereColumn("id").isEqualTo(QueryBuilder.bindMarker("id"));
   * }</pre>
   *
   * All components of the primary key are listed in the {@code WHERE} clause as bindable values
   * (the bind markers have the same names as the columns). They are listed in the natural order,
   * i.e. partition key columns first, followed by clustering columns (in the order defined by the
   * {@link PartitionKey} and {@link ClusteringColumn} annotations on the entity class).
   *
   * <p>The keyspace and table identifiers are those of the DAO that this helper was obtained from;
   * * if the DAO was built without a specific keyspace and table, the query doesn't specify a
   * keyspace, and the table name is inferred from the naming strategy.
   */
  @NonNull
  Delete deleteByPrimaryKey();

  /**
   * The keyspace used in the queries generated by this helper.
   *
   * <p>This is determined by the following rules:
   *
   * <ul>
   *   <li>If the DAO that this helper belongs to was created with a keyspace (see {@link
   *       DaoKeyspace}), use that;
   *   <li>Otherwise, if {@link Entity#defaultKeyspace()} is set for the entity class, use that;
   *   <li>Otherwise, return {@code null}.
   * </ul>
   */
  @Nullable
  CqlIdentifier getKeyspaceId();

  /**
   * The table used in the queries generated by this helper.
   *
   * <p>This is determined by the following rules:
   *
   * <ul>
   *   <li>If the DAO that this helper belongs to was created with a table (see {@link DaoTable}),
   *       use that;
   *   <li>Otherwise, if the entity class is annotated with {@link CqlName}, use that;
   *   <li>Otherwise, use the name of the entity class, transformed by its {@link NamingStrategy}.
   * </ul>
   */
  @NonNull
  CqlIdentifier getTableId();

  /** The class of the mapped entity. */
  @NonNull
  Class<EntityT> getEntityClass();
}
