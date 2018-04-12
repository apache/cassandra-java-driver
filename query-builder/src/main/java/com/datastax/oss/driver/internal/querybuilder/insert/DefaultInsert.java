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
package com.datastax.oss.driver.internal.querybuilder.insert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.JsonInsert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

public class DefaultInsert implements InsertInto, RegularInsert, JsonInsert {

  public enum MissingJsonBehavior {
    NULL,
    UNSET
  }

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final Term json;
  private final MissingJsonBehavior missingJsonBehavior;
  private final ImmutableMap<CqlIdentifier, Term> assignments;
  private final Object timestamp;
  private final boolean ifNotExists;

  public DefaultInsert(CqlIdentifier keyspace, CqlIdentifier table) {
    this(keyspace, table, null, null, ImmutableMap.of(), null, false);
  }

  public DefaultInsert(
      CqlIdentifier keyspace,
      CqlIdentifier table,
      Term json,
      MissingJsonBehavior missingJsonBehavior,
      ImmutableMap<CqlIdentifier, Term> assignments,
      Object timestamp,
      boolean ifNotExists) {
    // Note: the public API guarantees this, but check in case someone is calling the internal API
    // directly.
    Preconditions.checkArgument(
        json == null || assignments.isEmpty(), "JSON insert can't have regular assignments");
    this.keyspace = keyspace;
    this.table = table;
    this.json = json;
    this.missingJsonBehavior = missingJsonBehavior;
    this.assignments = assignments;
    this.timestamp = timestamp;
    this.ifNotExists = ifNotExists;
  }

  @Override
  public JsonInsert json(String json) {
    return new DefaultInsert(
        keyspace,
        table,
        QueryBuilderDsl.literal(json),
        missingJsonBehavior,
        ImmutableMap.of(),
        timestamp,
        ifNotExists);
  }

  @Override
  public JsonInsert json(BindMarker json) {
    return new DefaultInsert(
        keyspace, table, json, missingJsonBehavior, ImmutableMap.of(), timestamp, ifNotExists);
  }

  @Override
  public JsonInsert defaultNull() {
    return new DefaultInsert(
        keyspace, table, json, MissingJsonBehavior.NULL, ImmutableMap.of(), timestamp, ifNotExists);
  }

  @Override
  public JsonInsert defaultUnset() {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        MissingJsonBehavior.UNSET,
        ImmutableMap.of(),
        timestamp,
        ifNotExists);
  }

  @Override
  public RegularInsert value(CqlIdentifier columnId, Term value) {
    return new DefaultInsert(
        keyspace,
        table,
        null,
        null,
        ImmutableCollections.append(assignments, columnId, value),
        timestamp,
        ifNotExists);
  }

  @Override
  public Insert ifNotExists() {
    return new DefaultInsert(
        keyspace, table, json, missingJsonBehavior, assignments, timestamp, true);
  }

  @Override
  public Insert usingTimestamp(long timestamp) {
    return new DefaultInsert(
        keyspace, table, json, missingJsonBehavior, assignments, timestamp, ifNotExists);
  }

  @Override
  public Insert usingTimestamp(BindMarker timestamp) {
    return new DefaultInsert(
        keyspace, table, json, missingJsonBehavior, assignments, timestamp, ifNotExists);
  }

  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("INSERT INTO ");
    CqlHelper.qualify(keyspace, table, builder);

    if (json == null) {
      CqlHelper.appendIds(assignments.keySet(), builder, " (", ",", ")");
      CqlHelper.append(assignments.values(), builder, " VALUES (", ",", ")");
    } else {
      builder.append(" JSON ");
      json.appendTo(builder);
      if (missingJsonBehavior == MissingJsonBehavior.NULL) {
        builder.append(" DEFAULT NULL");
      } else if (missingJsonBehavior == MissingJsonBehavior.UNSET) {
        builder.append(" DEFAULT UNSET");
      }
    }
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    if (timestamp != null) {
      builder.append(" USING TIMESTAMP ");
      if (timestamp instanceof BindMarker) {
        ((BindMarker) timestamp).appendTo(builder);
      } else {
        builder.append(timestamp);
      }
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
    if (ifNotExists) {
      return false;
    } else {
      for (Term value : assignments.values()) {
        if (!value.isIdempotent()) {
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

  public Object getJson() {
    return json;
  }

  public MissingJsonBehavior getMissingJsonBehavior() {
    return missingJsonBehavior;
  }

  public ImmutableMap<CqlIdentifier, Term> getAssignments() {
    return assignments;
  }

  public Object getTimestamp() {
    return timestamp;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
