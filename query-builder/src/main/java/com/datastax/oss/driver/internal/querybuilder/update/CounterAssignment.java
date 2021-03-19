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
package com.datastax.oss.driver.internal.querybuilder.update;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class CounterAssignment implements Assignment {

  public enum Operator {
    INCREMENT("%1$s=%1$s+%2$s"),
    DECREMENT("%1$s=%1$s-%2$s"),
    ;

    public final String pattern;

    Operator(String pattern) {
      this.pattern = pattern;
    }
  }

  private final CqlIdentifier columnId;
  private final Operator operator;
  private final Term value;

  protected CounterAssignment(
      @NonNull CqlIdentifier columnId, @NonNull Operator operator, @NonNull Term value) {
    Preconditions.checkNotNull(columnId);
    Preconditions.checkNotNull(value);
    this.columnId = columnId;
    this.operator = operator;
    this.value = value;
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(String.format(operator.pattern, columnId.asCql(true), buildRightOperand()));
  }

  private String buildRightOperand() {
    StringBuilder builder = new StringBuilder();
    value.appendTo(builder);
    return builder.toString();
  }

  @Override
  public boolean isIdempotent() {
    return false;
  }

  @NonNull
  public CqlIdentifier getColumnId() {
    return columnId;
  }

  @NonNull
  public Term getValue() {
    return value;
  }
}
