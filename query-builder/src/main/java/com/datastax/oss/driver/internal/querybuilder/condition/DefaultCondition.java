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
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.LeftOperand;

public class DefaultCondition implements Condition {

  private final LeftOperand leftOperand;
  private final String operator;
  private final Term rightOperand;

  public DefaultCondition(LeftOperand leftOperand, String operator, Term rightOperand) {
    this.leftOperand = leftOperand;
    this.operator = operator;
    this.rightOperand = rightOperand;
  }

  @Override
  public void appendTo(StringBuilder builder) {
    leftOperand.appendTo(builder);
    builder.append(operator);
    if (rightOperand != null) {
      rightOperand.appendTo(builder);
    }
  }

  public LeftOperand getLeftOperand() {
    return leftOperand;
  }

  public String getOperator() {
    return operator;
  }

  public Term getRightOperand() {
    return rightOperand;
  }
}
