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

public class DefaultUpdate implements UpdateStart, UpdateWithAssignments, Update {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final Object timestamp;
  private final ImmutableList<Assignment> assignments;
  private final ImmutableList<Relation> relations;
  private final boolean ifExists;
  private final ImmutableList<Condition> conditions;

  public DefaultUpdate(CqlIdentifier keyspace, CqlIdentifier table) {
    this(keyspace, table, null, ImmutableList.of(), ImmutableList.of(), false, ImmutableList.of());
  }

  public DefaultUpdate(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      Object timestamp,
      ImmutableList<Assignment> assignments,
      ImmutableList<Relation> relations,
      boolean ifExists,
      ImmutableList<Condition> conditions) {
    this.keyspace = keyspace;
    this.table = table;
    this.timestamp = timestamp;
    this.assignments = assignments;
    this.relations = relations;
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @Override
  public UpdateStart usingTimestamp(long newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, assignments, relations, ifExists, conditions);
  }

  @Override
  public UpdateStart usingTimestamp(BindMarker newTimestamp) {
    return new DefaultUpdate(
        keyspace, table, newTimestamp, assignments, relations, ifExists, conditions);
  }

  @Override
  public UpdateWithAssignments set(Assignment assignment) {
    return withAssignments(ImmutableCollections.append(assignments, assignment));
  }

  @Override
  public UpdateWithAssignments set(Iterable<Assignment> additionalAssignments) {
    return withAssignments(ImmutableCollections.concat(assignments, additionalAssignments));
  }

  public UpdateWithAssignments withAssignments(ImmutableList<Assignment> newAssignments) {
    return new DefaultUpdate(
        keyspace, table, timestamp, newAssignments, relations, ifExists, conditions);
  }

  @Override
  public Update where(Relation relation) {
    return withRelations(ImmutableCollections.append(relations, relation));
  }

  @Override
  public Update where(Iterable<Relation> additionalRelations) {
    return withRelations(ImmutableCollections.concat(relations, additionalRelations));
  }

  public Update withRelations(ImmutableList<Relation> newRelations) {
    return new DefaultUpdate(
        keyspace, table, timestamp, assignments, newRelations, ifExists, conditions);
  }

  @Override
  public Update ifExists() {
    return new DefaultUpdate(keyspace, table, timestamp, assignments, relations, true, conditions);
  }

  @Override
  public Update if_(Condition condition) {
    return withConditions(ImmutableCollections.append(conditions, condition));
  }

  @Override
  public Update if_(Iterable<Condition> additionalConditions) {
    return withConditions(ImmutableCollections.concat(conditions, additionalConditions));
  }

  public Update withConditions(ImmutableList<Condition> newConditions) {
    return new DefaultUpdate(
        keyspace, table, timestamp, assignments, relations, false, newConditions);
  }

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

  @Override
  public SimpleStatement build() {
    return builder().build();
  }

  @Override
  public SimpleStatementBuilder builder() {
    return SimpleStatement.builder(asCql()).withIdempotence(isIdempotent());
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

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CqlIdentifier getTable() {
    return table;
  }

  public Object getTimestamp() {
    return timestamp;
  }

  public ImmutableList<Assignment> getAssignments() {
    return assignments;
  }

  public ImmutableList<Relation> getRelations() {
    return relations;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public ImmutableList<Condition> getConditions() {
    return conditions;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
