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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnComponentRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnComponentLeftOperand;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultColumnComponentRelationBuilder
    implements ColumnComponentRelationBuilder<Relation> {

  private final CqlIdentifier columnId;
  private final Term index;

  public DefaultColumnComponentRelationBuilder(
      @NonNull CqlIdentifier columnId, @NonNull Term index) {
    this.columnId = columnId;
    this.index = index;
  }

  @NonNull
  @Override
  public Relation build(@NonNull String operator, @Nullable Term rightOperand) {
    return new DefaultRelation(
        new ColumnComponentLeftOperand(columnId, index), operator, rightOperand);
  }

  @Immutable
  public static class Fluent<StatementT extends OngoingWhereClause<StatementT>>
      implements ColumnComponentRelationBuilder<StatementT> {

    private final OngoingWhereClause<StatementT> statement;
    private final ColumnComponentRelationBuilder<Relation> delegate;

    public Fluent(
        @NonNull OngoingWhereClause<StatementT> statement,
        @NonNull CqlIdentifier columnId,
        @NonNull Term index) {
      this.statement = statement;
      this.delegate = new DefaultColumnComponentRelationBuilder(columnId, index);
    }

    @NonNull
    @Override
    public StatementT build(@NonNull String operator, @Nullable Term rightOperand) {
      return statement.where(delegate.build(operator, rightOperand));
    }
  }
}
