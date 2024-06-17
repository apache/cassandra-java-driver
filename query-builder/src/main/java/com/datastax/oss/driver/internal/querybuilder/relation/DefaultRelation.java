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

import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.lhs.LeftOperand;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultRelation implements Relation {

  private final LeftOperand leftOperand;
  private final String operator;
  private final Term rightOperand;

  public DefaultRelation(
      @NonNull LeftOperand leftOperand, @NonNull String operator, @Nullable Term rightOperand) {
    Preconditions.checkNotNull(leftOperand);
    Preconditions.checkNotNull(operator);
    this.leftOperand = leftOperand;
    this.operator = operator;
    this.rightOperand = rightOperand;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    leftOperand.appendTo(builder);
    builder.append(operator);
    if (rightOperand != null) {
      rightOperand.appendTo(builder);
    }
  }

  @Override
  public boolean isIdempotent() {
    return rightOperand == null || rightOperand.isIdempotent();
  }

  @NonNull
  public LeftOperand getLeftOperand() {
    return leftOperand;
  }

  @NonNull
  public String getOperator() {
    return operator;
  }

  @Nullable
  public Term getRightOperand() {
    return rightOperand;
  }
}
