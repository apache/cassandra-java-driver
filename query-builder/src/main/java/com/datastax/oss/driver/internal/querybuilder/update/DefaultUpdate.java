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
package com.datastax.oss.driver.internal.querybuilder.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultUpdate implements UpdateStart, UpdateWithAssignments, Update {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final Object timestamp;
  private final Object ttlInSeconds;
  private final ImmutableList<Assignment> assignments;
  private final ImmutableList<Relation> relations;
  private final boolean ifExists;
  private final ImmutableList<Condition> conditions;

  public DefaultUpdate(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier table) {
    this(
        keyspace,
        table,
        null,
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        false,
        ImmutableList.of());
  }

  public DefaultUpdate(
      @Nullable CqlIdentifier keyspace,
      @Nonnull CqlIdentifier table,
      @Nullable Object timestamp,
      @Nullable Object ttlInSeconds,
      @Nonnull ImmutableList<Assignment> assignments,
      @Nonnull ImmutableList<Relation> relations,
      boolean ifExists,
      @Nonnull ImmutableList<Condition> conditions) {
    Preconditions.checkArgument(
        timestamp == null || timestamp instanceof Long || timestamp instanceof BindMarker,
        "TIMESTAMP value must be a BindMarker or a Long");
    Preconditions.checkArgument(
        ttlInSeconds == null
            || ttlInSeconds instanceof Integer
            || ttlInSeconds instanceof BindMarker,
        "TTL value must be a BindMarker or an Integer");

    this.keyspace = keyspace;
    this.table = table;
    this.timestamp = timestamp;
    this.ttlInSeconds = ttlInSeconds;
    this.assignments = assignments;
    this.relations = relations;
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @Nonnull
  @Override
  public UpdateStart usingTimestamp(long newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, ttlInSeconds, assignments, relations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public UpdateStart usingTimestamp(@Nonnull BindMarker newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, ttlInSeconds, assignments, relations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public UpdateStart usingTtl(int ttlInSeconds) {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, assignments, relations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public UpdateStart usingTtl(@Nonnull BindMarker ttlInSeconds) {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, assignments, relations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public UpdateWithAssignments set(@Nonnull Assignment assignment) {
    return withAssignments(ImmutableCollections.append(assignments, assignment));
  }

  @Nonnull
  @Override
  public UpdateWithAssignments set(@Nonnull Iterable<Assignment> additionalAssignments) {
    return withAssignments(ImmutableCollections.concat(assignments, additionalAssignments));
  }

  @Nonnull
  public UpdateWithAssignments withAssignments(@Nonnull ImmutableList<Assignment> newAssignments) {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, newAssignments, relations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public Update where(@Nonnull Relation relation) {
    return withRelations(ImmutableCollections.append(relations, relation));
  }

  @Nonnull
  @Override
  public Update where(@Nonnull Iterable<Relation> additionalRelations) {
    return withRelations(ImmutableCollections.concat(relations, additionalRelations));
  }

  @Nonnull
  public Update withRelations(@Nonnull ImmutableList<Relation> newRelations) {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, assignments, newRelations, ifExists, conditions);
  }

  @Nonnull
  @Override
  public Update ifExists() {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, assignments, relations, true, conditions);
  }

  @Nonnull
  @Override
  public Update if_(@Nonnull Condition condition) {
    return withConditions(ImmutableCollections.append(conditions, condition));
  }

  @Nonnull
  @Override
  public Update if_(@Nonnull Iterable<Condition> additionalConditions) {
    return withConditions(ImmutableCollections.concat(conditions, additionalConditions));
  }

  @Nonnull
  public Update withConditions(@Nonnull ImmutableList<Condition> newConditions) {
    return new DefaultUpdate(
        keyspace, table, timestamp, ttlInSeconds, assignments, relations, false, newConditions);
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("UPDATE ");
    CqlHelper.qualify(keyspace, table, builder);

    if (timestamp != null) {
      builder.append(" USING TIMESTAMP ");
      if (timestamp instanceof BindMarker) {
        ((BindMarker) timestamp).appendTo(builder);
      } else {
        builder.append(timestamp);
      }
    }

    if (ttlInSeconds != null) {
      // choose the correct keyword based on whether or not we have a timestamp
      builder.append((timestamp != null) ? " AND " : " USING ").append("TTL ");
      if (ttlInSeconds instanceof BindMarker) {
        ((BindMarker) ttlInSeconds).appendTo(builder);
      } else {
        builder.append(ttlInSeconds);
      }
    }

    CqlHelper.append(assignments, builder, " SET ", ", ", null);
    CqlHelper.append(relations, builder, " WHERE ", " AND ", null);

    if (ifExists) {
      builder.append(" IF EXISTS");
    } else {
      CqlHelper.append(conditions, builder, " IF ", " AND ", null);
    }
    return builder.toString();
  }

  @Nonnull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }

  @Nonnull
  @Override
  public SimpleStatement build(@Nonnull Object... values) {
    return builder().addPositionalValues(values).build();
  }

  @Nonnull
  @Override
  public SimpleStatement build(@Nonnull Map<String, Object> namedValues) {
    SimpleStatementBuilder builder = builder();
    for (Map.Entry<String, Object> entry : namedValues.entrySet()) {
      builder.addNamedValue(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  @Nonnull
  @Override
  public SimpleStatementBuilder builder() {
    return SimpleStatement.builder(asCql()).setIdempotence(isIdempotent());
  }

  public boolean isIdempotent() {
    // Conditional queries are never idempotent, see JAVA-819
    if (!conditions.isEmpty() || ifExists) {
      return false;
    } else {
      for (Assignment assignment : assignments) {
        if (!assignment.isIdempotent()) {
          return false;
        }
      }
      for (Relation relation : relations) {
        if (!relation.isIdempotent()) {
          return false;
        }
      }
      return true;
    }
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  public CqlIdentifier getTable() {
    return table;
  }

  @Nullable
  public Object getTimestamp() {
    return timestamp;
  }

  @Nullable
  public Object getTtl() {
    return ttlInSeconds;
  }

  @Nonnull
  public ImmutableList<Assignment> getAssignments() {
    return assignments;
  }

  @Nonnull
  public ImmutableList<Relation> getRelations() {
    return relations;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  @Nonnull
  public ImmutableList<Condition> getConditions() {
    return conditions;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
