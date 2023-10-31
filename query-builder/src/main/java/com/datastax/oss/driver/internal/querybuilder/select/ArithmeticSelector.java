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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.ArithmeticOperator;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import javax.annotation.Nonnull;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class ArithmeticSelector implements Selector {

  protected final ArithmeticOperator operator;

  protected ArithmeticSelector(@Nonnull ArithmeticOperator operator) {
    Preconditions.checkNotNull(operator);
    this.operator = operator;
  }

  @Nonnull
  public ArithmeticOperator getOperator() {
    return operator;
  }

  protected static void appendAndMaybeParenthesize(
      int myPrecedence, @Nonnull Selector child, @Nonnull StringBuilder builder) {
    boolean parenthesize =
        (child instanceof ArithmeticSelector)
            && (((ArithmeticSelector) child).operator.getPrecedenceLeft() < myPrecedence);
    if (parenthesize) {
      builder.append('(');
    }
    child.appendTo(builder);
    if (parenthesize) {
      builder.append(')');
    }
  }
}
