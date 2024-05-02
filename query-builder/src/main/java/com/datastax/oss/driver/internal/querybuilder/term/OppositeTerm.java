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
package com.datastax.oss.driver.internal.querybuilder.term;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.ArithmeticOperator;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class OppositeTerm extends ArithmeticTerm {

  @NonNull private final Term argument;

  public OppositeTerm(@NonNull Term argument) {
    super(ArithmeticOperator.OPPOSITE);
    Preconditions.checkNotNull(argument);
    this.argument = argument;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append('-');
    appendAndMaybeParenthesize(operator.getPrecedenceLeft(), argument, builder);
  }

  @Override
  public boolean isIdempotent() {
    return argument.isIdempotent();
  }

  @NonNull
  public Term getArgument() {
    return argument;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof OppositeTerm) {
      OppositeTerm that = (OppositeTerm) other;
      return this.argument.equals(that.argument);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return argument.hashCode();
  }
}
