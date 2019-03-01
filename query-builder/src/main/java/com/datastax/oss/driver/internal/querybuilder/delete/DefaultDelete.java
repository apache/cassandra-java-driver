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
package com.datastax.oss.driver.internal.querybuilder.delete;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.internal.querybuilder.select.ElementSelector;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDelete implements DeleteSelection, Delete {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final ImmutableList<Selector> selectors;
  private final ImmutableList<Relation> relations;
  private final Object timestamp;
  private final boolean ifExists;
  private final ImmutableList<Condition> conditions;

  public DefaultDelete(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this(keyspace, table, ImmutableList.of(), ImmutableList.of(), null, false, ImmutableList.of());
  }

  public DefaultDelete(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      @NonNull ImmutableList<Selector> selectors,
      @NonNull ImmutableList<Relation> relations,
      @Nullable Object timestamp,
      boolean ifExists,
      @NonNull ImmutableList<Condition> conditions) {
    this.keyspace = keyspace;
    this.table = table;
    this.selectors = selectors;
    this.relations = relations;
    this.timestamp = timestamp;
    this.ifExists = ifExists;
    this.conditions = conditions;
  }

  @NonNull
  @Override
  public DeleteSelection selector(@NonNull Selector selector) {
    return withSelectors(ImmutableCollections.append(selectors, selector));
  }

  @NonNull
  @Override
  public DeleteSelection selectors(@NonNull Iterable<Selector> additionalSelectors) {
    return withSelectors(ImmutableCollections.concat(selectors, additionalSelectors));
  }

  @NonNull
  public DeleteSelection withSelectors(@NonNull ImmutableList<Selector> newSelectors) {
    return new DefaultDelete(
        keyspace, table, newSelectors, relations, timestamp, ifExists, conditions);
  }

  @NonNull
  @Override
  public Delete where(@NonNull Relation relation) {
    return withRelations(ImmutableCollections.append(relations, relation));
  }

  @NonNull
  @Override
  public Delete where(@NonNull Iterable<Relation> additionalRelations) {
    return withRelations(ImmutableCollections.concat(relations, additionalRelations));
  }

  @NonNull
  public Delete withRelations(@NonNull ImmutableList<Relation> newRelations) {
    return new DefaultDelete(
        keyspace, table, selectors, newRelations, timestamp, ifExists, conditions);
  }

  @NonNull
  @Override
  public DeleteSelection usingTimestamp(long newTimestamp) {
    return new DefaultDelete(
        keyspace, table, selectors, relations, newTimestamp, ifExists, conditions);
  }

  @NonNull
  @Override
  public DeleteSelection usingTimestamp(@Nullable BindMarker newTimestamp) {
    return new DefaultDelete(
        keyspace, table, selectors, relations, newTimestamp, ifExists, conditions);
  }

  @NonNull
  @Override
  public Delete ifExists() {
    return new DefaultDelete(
        keyspace, table, selectors, relations, timestamp, true, ImmutableList.of());
  }

  @NonNull
  @Override
  public Delete if_(@NonNull Condition condition) {
    return withConditions(ImmutableCollections.append(conditions, condition));
  }

  @NonNull
  @Override
  public Delete if_(@NonNull Iterable<Condition> additionalConditions) {
    return withConditions(ImmutableCollections.concat(conditions, additionalConditions));
  }

  @NonNull
  public Delete withConditions(@NonNull ImmutableList<Condition> newConditions) {
    return new DefaultDelete(
        keyspace, table, selectors, relations, timestamp, false, newConditions);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("DELETE");

    CqlHelper.append(selectors, builder, " ", ",", null);

    builder.append(" FROM ");
    CqlHelper.qualify(keyspace, table, builder);

    if (timestamp != null) {
      builder.append(" USING TIMESTAMP ");
      if (timestamp instanceof BindMarker) {
        ((BindMarker) timestamp).appendTo(builder);
      } else {
        builder.append(timestamp);
      }
    }

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
      for (Selector selector : selectors) {
        // `DELETE list[0]` is not idempotent. Unfortunately we don't know what type of collection
        // an elements selector targets, so be conservative.
        if (selector instanceof ElementSelector) {
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

  @NonNull
  public ImmutableList<Selector> getSelectors() {
    return selectors;
  }

  @NonNull
  public ImmutableList<Relation> getRelations() {
    return relations;
  }

  @Nullable
  public Object getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
