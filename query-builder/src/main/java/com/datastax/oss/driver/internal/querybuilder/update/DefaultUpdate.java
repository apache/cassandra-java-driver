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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.querybuilder.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.data.CqlDuration;
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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultUpdate implements UpdateStart, UpdateWithAssignments, Update {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final Object timestamp;
  private final Object ttlInSeconds;
  private final Object timeout;
  private final ImmutableList<Assignment> assignments;
  private final ImmutableList<Relation> relations;
  private final boolean ifExists;
  private final ImmutableList<Condition> conditions;

  public DefaultUpdate(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this(
        keyspace,
        table,
        null,
        null,
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        false,
        ImmutableList.of());
  }

  public DefaultUpdate(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @Nullable Object timestamp,
      @Nullable Object ttlInSeconds,
      @Nullable Object timeout,
      @NonNull ImmutableList<Assignment> assignments,
      @NonNull ImmutableList<Relation> relations,
      boolean ifExists,
      @NonNull ImmutableList<Condition> conditions) {
    Preconditions.checkArgument(
        timestamp == null || timestamp instanceof Long || timestamp instanceof BindMarker,
        "TIMESTAMP value must be a BindMarker or a Long");
    Preconditions.checkArgument(
        ttlInSeconds == null
            || ttlInSeconds instanceof Integer
            || ttlInSeconds instanceof BindMarker,
        "TTL value must be a BindMarker or an Integer");
    Preconditions.checkArgument(
        timeout == null || timeout instanceof CqlDuration || timeout instanceof BindMarker,
        "TIMEOUT value must be a BindMarker or a CqlDuration");
    this.keyspace = keyspace;
    this.table = table;
    this.timestamp = timestamp;
    this.ttlInSeconds = ttlInSeconds;
    this.timeout = timeout;
    this.assignments = assignments;
    this.relations = relations;
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @NonNull
  @Override
  public UpdateStart usingTimestamp(long newTimestamp) {
    return new DefaultUpdate(
        keyspace,
        table,
        newTimestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTimestamp(@NonNull BindMarker newTimestamp) {
    return new DefaultUpdate(
        keyspace,
        table,
        newTimestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTtl(int ttlInSeconds) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTtl(@NonNull BindMarker ttlInSeconds) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTimeout(@NonNull CqlDuration timeout) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTimeout(@NonNull BindMarker timeout) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public UpdateWithAssignments set(@NonNull Assignment assignment) {
    return withAssignments(ImmutableCollections.append(assignments, assignment));
  }

  @NonNull
  @Override
  public UpdateWithAssignments set(@NonNull Iterable<Assignment> additionalAssignments) {
    return withAssignments(ImmutableCollections.concat(assignments, additionalAssignments));
  }

  @NonNull
  public UpdateWithAssignments withAssignments(@NonNull ImmutableList<Assignment> newAssignments) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        newAssignments,
        relations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public Update where(@NonNull Relation relation) {
    return withRelations(ImmutableCollections.append(relations, relation));
  }

  @NonNull
  @Override
  public Update where(@NonNull Iterable<Relation> additionalRelations) {
    return withRelations(ImmutableCollections.concat(relations, additionalRelations));
  }

  @NonNull
  public Update withRelations(@NonNull ImmutableList<Relation> newRelations) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        newRelations,
        ifExists,
        conditions);
  }

  @NonNull
  @Override
  public Update ifExists() {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        true,
        conditions);
  }

  @NonNull
  @Override
  public Update if_(@NonNull Condition condition) {
    return withConditions(ImmutableCollections.append(conditions, condition));
  }

  @NonNull
  @Override
  public Update if_(@NonNull Iterable<Condition> additionalConditions) {
    return withConditions(ImmutableCollections.concat(conditions, additionalConditions));
  }

  @NonNull
  public Update withConditions(@NonNull ImmutableList<Condition> newConditions) {
    return new DefaultUpdate(
        keyspace,
        table,
        timestamp,
        ttlInSeconds,
        timeout,
        assignments,
        relations,
        false,
        newConditions);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("UPDATE ");
    CqlHelper.qualify(keyspace, table, builder);

    boolean hasUsing = false;
    if (timestamp != null) {
      builder.append(" USING TIMESTAMP ");
      hasUsing = true;
      if (timestamp instanceof BindMarker) {
        ((BindMarker) timestamp).appendTo(builder);
      } else {
        builder.append(timestamp);
      }
    }

    if (ttlInSeconds != null) {
      // choose the correct keyword based on whether or not we have a timestamp
      builder.append(hasUsing ? " AND " : " USING ").append("TTL ");
      hasUsing = true;
      if (ttlInSeconds instanceof BindMarker) {
        ((BindMarker) ttlInSeconds).appendTo(builder);
      } else {
        builder.append(ttlInSeconds);
      }
    }

    if (timeout != null) {
      builder.append(hasUsing ? " AND " : " USING ").append("TIMEOUT ");
      hasUsing = true;
      if (timeout instanceof BindMarker) {
        ((BindMarker) timeout).appendTo(builder);
      } else {
        ((CqlDuration) timeout).appendTo(builder);
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

  @NonNull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Object... values) {
    return builder().addPositionalValues(values).build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
    SimpleStatementBuilder builder = builder();
    for (Map.Entry<String, Object> entry : namedValues.entrySet()) {
      builder.addNamedValue(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  @NonNull
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

  @NonNull
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

  @Nullable
  public Object getTimeout() {
    return timeout;
  }

  @NonNull
  public ImmutableList<Assignment> getAssignments() {
    return assignments;
  }

  @NonNull
  public ImmutableList<Relation> getRelations() {
    return relations;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  @NonNull
  public ImmutableList<Condition> getConditions() {
    return conditions;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
