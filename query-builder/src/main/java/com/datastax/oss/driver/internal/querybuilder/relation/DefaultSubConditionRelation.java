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
package com.datastax.oss.driver.internal.querybuilder.relation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultSubConditionRelation
    implements OngoingWhereClause<DefaultSubConditionRelation>, BuildableQuery, Relation {

  private final List<Relation> relations;
  private final boolean isSubCondition;

  /** Construct sub-condition relation with empty WHERE clause. */
  public DefaultSubConditionRelation(boolean isSubCondition) {
    this.relations = new ArrayList<>();
    this.isSubCondition = isSubCondition;
  }

  @NonNull
  @Override
  public DefaultSubConditionRelation where(@NonNull Relation relation) {
    relations.add(relation);
    return this;
  }

  @NonNull
  @Override
  public DefaultSubConditionRelation where(@NonNull Iterable<Relation> additionalRelations) {
    for (Relation relation : additionalRelations) {
      relations.add(relation);
    }
    return this;
  }

  @NonNull
  public DefaultSubConditionRelation withRelations(@NonNull List<Relation> newRelations) {
    relations.addAll(newRelations);
    return this;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    if (isSubCondition) {
      builder.append("(");
    }
    appendWhereClause(builder, relations, isSubCondition);
    if (isSubCondition) {
      builder.append(")");
    }

    return builder.toString();
  }

  public static void appendWhereClause(
      StringBuilder builder, List<Relation> relations, boolean isSubCondition) {
    boolean first = true;
    for (int i = 0; i < relations.size(); ++i) {
      CqlSnippet snippet = relations.get(i);
      if (first && !isSubCondition) {
        builder.append(" WHERE ");
      }
      first = false;

      snippet.appendTo(builder);

      boolean logicalOperatorAdded = false;
      LogicalRelation logicalRelation = lookAheadNextRelation(relations, i, LogicalRelation.class);
      if (logicalRelation != null) {
        builder.append(" ");
        logicalRelation.appendTo(builder);
        builder.append(" ");
        logicalOperatorAdded = true;
        ++i;
      }
      if (!logicalOperatorAdded && i + 1 < relations.size()) {
        builder.append(" AND ");
      }
    }
  }

  private static <T extends Relation> T lookAheadNextRelation(
      List<Relation> relations, int position, Class<T> clazz) {
    if (position + 1 >= relations.size()) {
      return null;
    }
    Relation relation = relations.get(position + 1);
    if (relation.getClass().isAssignableFrom(clazz)) {
      return (T) relation;
    }
    return null;
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

  @Override
  public String toString() {
    return asCql();
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(asCql());
  }

  @Override
  public boolean isIdempotent() {
    for (Relation relation : relations) {
      if (!relation.isIdempotent()) {
        return false;
      }
    }
    return true;
  }
}
