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
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.JsonInsert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
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
  private final Object ttlInSeconds;
  private final boolean ifNotExists;

  public DefaultInsert(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this(keyspace, table, null, null, ImmutableMap.of(), null, null, false);
  }

  public DefaultInsert(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @Nullable Term json,
      @Nullable MissingJsonBehavior missingJsonBehavior,
      @NonNull ImmutableMap<CqlIdentifier, Term> assignments,
      @Nullable Object timestamp,
      @Nullable Object ttlInSeconds,
      boolean ifNotExists) {
    // Note: the public API guarantees this, but check in case someone is calling the internal API
    // directly.
    Preconditions.checkArgument(
        json == null || assignments.isEmpty(), "JSON insert can't have regular assignments");
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
    this.json = json;
    this.missingJsonBehavior = missingJsonBehavior;
    this.assignments = assignments;
    this.timestamp = timestamp;
    this.ttlInSeconds = ttlInSeconds;
    this.ifNotExists = ifNotExists;
  }

  @NonNull
  @Override
  public JsonInsert json(@NonNull String json) {
    return new DefaultInsert(
        keyspace,
        table,
        QueryBuilder.literal(json),
        missingJsonBehavior,
        ImmutableMap.of(),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public JsonInsert json(@NonNull BindMarker json) {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        missingJsonBehavior,
        ImmutableMap.of(),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public <T> JsonInsert json(@NonNull T value, @NonNull TypeCodec<T> codec) {
    return new DefaultInsert(
        keyspace,
        table,
        QueryBuilder.literal(value, codec),
        missingJsonBehavior,
        ImmutableMap.of(),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public JsonInsert defaultNull() {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        MissingJsonBehavior.NULL,
        ImmutableMap.of(),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public JsonInsert defaultUnset() {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        MissingJsonBehavior.UNSET,
        ImmutableMap.of(),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public RegularInsert value(@NonNull CqlIdentifier columnId, @NonNull Term value) {
    return new DefaultInsert(
        keyspace,
        table,
        null,
        null,
        ImmutableCollections.append(assignments, columnId, value),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public RegularInsert valuesByIds(@NonNull Map<CqlIdentifier, Term> newAssignments) {
    return new DefaultInsert(
        keyspace,
        table,
        null,
        null,
        ImmutableCollections.concat(assignments, newAssignments),
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public Insert ifNotExists() {
    return new DefaultInsert(
        keyspace, table, json, missingJsonBehavior, assignments, timestamp, ttlInSeconds, true);
  }

  @NonNull
  @Override
  public Insert usingTimestamp(long timestamp) {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        missingJsonBehavior,
        assignments,
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public Insert usingTimestamp(@Nullable BindMarker timestamp) {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        missingJsonBehavior,
        assignments,
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public Insert usingTtl(int ttlInSeconds) {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        missingJsonBehavior,
        assignments,
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
  @Override
  public Insert usingTtl(@Nullable BindMarker ttlInSeconds) {
    return new DefaultInsert(
        keyspace,
        table,
        json,
        missingJsonBehavior,
        assignments,
        timestamp,
        ttlInSeconds,
        ifNotExists);
  }

  @NonNull
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
    if (ttlInSeconds != null) {
      builder.append((timestamp != null) ? " AND " : " USING ").append("TTL ");
      if (ttlInSeconds instanceof BindMarker) {
        ((BindMarker) ttlInSeconds).appendTo(builder);
      } else {
        builder.append(ttlInSeconds);
      }
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

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getTable() {
    return table;
  }

  @Nullable
  public Object getJson() {
    return json;
  }

  @Nullable
  public MissingJsonBehavior getMissingJsonBehavior() {
    return missingJsonBehavior;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, Term> getAssignments() {
    return assignments;
  }

  @Nullable
  public Object getTimestamp() {
    return timestamp;
  }

  @Nullable
  public Object getTtlInSeconds() {
    return ttlInSeconds;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
