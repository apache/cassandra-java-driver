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
package com.datastax.oss.driver.internal.querybuilder.condition;

import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.condition.ConditionBuilder;
import com.datastax.oss.driver.api.querybuilder.condition.ConditionalStatement;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.LeftOperand;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultConditionBuilder implements ConditionBuilder<Condition> {

  private final LeftOperand leftOperand;

  public DefaultConditionBuilder(@NonNull LeftOperand leftOperand) {
    this.leftOperand = leftOperand;
  }

  @NonNull
  @Override
  public Condition build(@NonNull String operator, @Nullable Term rightOperand) {
    return new DefaultCondition(leftOperand, operator, rightOperand);
  }

  @Immutable
  public static class Fluent<StatementT extends ConditionalStatement<StatementT>>
      implements ConditionBuilder<StatementT> {

    private final ConditionalStatement<StatementT> statement;
    private final ConditionBuilder<Condition> delegate;

    public Fluent(
        @NonNull ConditionalStatement<StatementT> statement, @NonNull LeftOperand leftOperand) {
      this.statement = statement;
      this.delegate = new DefaultConditionBuilder(leftOperand);
    }

    @NonNull
    @Override
    public StatementT build(@NonNull String operator, @Nullable Term rightOperand) {
      return statement.if_(delegate.build(operator, rightOperand));
    }
  }
}
