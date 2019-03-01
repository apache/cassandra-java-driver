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
  private final ImmutableList<Assignment> assignments;
  private final ImmutableList<Relation> relations;
  private final boolean ifExists;
  private final ImmutableList<Condition> conditions;

  public DefaultUpdate(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this(keyspace, table, null, ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of());
  }

  public DefaultUpdate(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @Nullable Object timestamp,
      @NonNull ImmutableList<Assignment> assignments,
      @NonNull ImmutableList<Relation> relations,
      boolean ifExists,
      @NonNull ImmutableList<Condition> conditions) {
    this.keyspace = keyspace;
    this.table = table;
    this.timestamp = timestamp;
    this.assignments = assignments;
    this.relations = relations;
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @NonNull
  @Override
  public UpdateStart usingTimestamp(long newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, assignments, relations, ifExists, conditions);
  }

  @NonNull
  @Override
  public UpdateStart usingTimestamp(@NonNull BindMarker newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, assignments, relations, ifExists, conditions);
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
        keyspace, table, timestamp, newAssignments, relations, ifExists, conditions);
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
        keyspace, table, timestamp, assignments, newRelations, ifExists, conditions);
  }

  @NonNull
  @Override
  public Update ifExists() {
    return new DefaultUpdate(keyspace, table, timestamp, assignments, relations, true, conditions);
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
        keyspace, table, timestamp, assignments, relations, false, newConditions);
  }

  @NonNull
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
