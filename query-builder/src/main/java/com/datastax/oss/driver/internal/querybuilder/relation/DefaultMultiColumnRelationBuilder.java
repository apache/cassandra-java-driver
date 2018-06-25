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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultMultiColumnRelationBuilder implements MultiColumnRelationBuilder<Relation> {

  private final Iterable<CqlIdentifier> identifiers;

  public DefaultMultiColumnRelationBuilder(@NonNull Iterable<CqlIdentifier> identifiers) {
    Preconditions.checkNotNull(identifiers);
    Preconditions.checkArgument(
        identifiers.iterator().hasNext(), "Tuple must contain at least one column");
    this.identifiers = identifiers;
  }

  @NonNull
  @Override
  public Relation build(@NonNull String operator, @Nullable Term rightOperand) {
    return new DefaultRelation(new TupleLeftOperand(identifiers), operator, rightOperand);
  }

  @Immutable
  public static class Fluent<StatementT extends OngoingWhereClause<StatementT>>
      implements MultiColumnRelationBuilder<StatementT> {

    private final OngoingWhereClause<StatementT> statement;
    private final MultiColumnRelationBuilder<Relation> delegate;

    public Fluent(
        @NonNull OngoingWhereClause<StatementT> statement,
        @NonNull Iterable<CqlIdentifier> identifiers) {
      this.statement = statement;
      this.delegate = new DefaultMultiColumnRelationBuilder(identifiers);
    }

    @NonNull
    @Override
    public StatementT build(@NonNull String operator, @Nullable Term rightOperand) {
      return statement.where(delegate.build(operator, rightOperand));
    }
  }
}
