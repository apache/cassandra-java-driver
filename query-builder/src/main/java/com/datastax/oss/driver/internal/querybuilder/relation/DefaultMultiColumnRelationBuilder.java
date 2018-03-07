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
package com.datastax.oss.driver.internal.querybuilder.relation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.relation.MultiColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.TupleLeftOperand;
import com.google.common.base.Preconditions;

public class DefaultMultiColumnRelationBuilder implements MultiColumnRelationBuilder<Relation> {

  private final Iterable<CqlIdentifier> identifiers;

  public DefaultMultiColumnRelationBuilder(Iterable<CqlIdentifier> identifiers) {
    Preconditions.checkNotNull(identifiers);
    Preconditions.checkArgument(
        identifiers.iterator().hasNext(), "Tuple must contain at least one column");
    this.identifiers = identifiers;
  }

  @Override
  public Relation build(String operator, Term rightOperand) {
    return new DefaultRelation(new TupleLeftOperand(identifiers), operator, rightOperand);
  }

  public static class Fluent<StatementT extends OngoingWhereClause<StatementT>>
      implements MultiColumnRelationBuilder<StatementT> {

    private final OngoingWhereClause<StatementT> statement;
    private final MultiColumnRelationBuilder<Relation> delegate;

    public Fluent(OngoingWhereClause<StatementT> statement, Iterable<CqlIdentifier> identifiers) {
      this.statement = statement;
      this.delegate = new DefaultMultiColumnRelationBuilder(identifiers);
    }

    @Override
    public StatementT build(String operator, Term rightOperand) {
      return statement.where(delegate.build(operator, rightOperand));
    }
  }
}
