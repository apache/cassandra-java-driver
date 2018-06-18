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
package com.datastax.oss.driver.internal.querybuilder.term;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.ArithmeticOperator;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class ArithmeticTerm implements Term {

  protected final ArithmeticOperator operator;

  protected ArithmeticTerm(@NonNull ArithmeticOperator operator) {
    Preconditions.checkNotNull(operator);
    this.operator = operator;
  }

  @NonNull
  public ArithmeticOperator getOperator() {
    return operator;
  }

  protected static void appendAndMaybeParenthesize(
      int myPrecedence, @NonNull Term child, @NonNull StringBuilder builder) {
    boolean parenthesize =
        (child instanceof ArithmeticTerm)
            && (((ArithmeticTerm) child).operator.getPrecedenceLeft() < myPrecedence);
    if (parenthesize) {
      builder.append('(');
    }
    child.appendTo(builder);
    if (parenthesize) {
      builder.append(')');
    }
  }
}
