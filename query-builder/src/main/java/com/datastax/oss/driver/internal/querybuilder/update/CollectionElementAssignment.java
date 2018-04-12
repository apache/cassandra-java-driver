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
import com.google.common.base.Preconditions;

public abstract class CollectionElementAssignment implements Assignment {

  public enum Operator {
    APPEND("%s+=%s"),
    PREPEND("%1$s=%2$s+%1$s"),
    REMOVE("%s-=%s"),
    ;

    public final String pattern;

    Operator(String pattern) {
      this.pattern = pattern;
    }
  }

  private final CqlIdentifier columnId;
  private final Operator operator;
  private final Term key;
  private final Term value;
  private final char opening;
  private final char closing;

  protected CollectionElementAssignment(
      CqlIdentifier columnId, Operator operator, Term key, Term value, char opening, char closing) {
    Preconditions.checkNotNull(columnId);
    Preconditions.checkNotNull(value);
    this.columnId = columnId;
    this.operator = operator;
    this.key = key;
    this.value = value;
    this.opening = opening;
    this.closing = closing;
  }

  @Override
  public void appendTo(StringBuilder builder) {
    builder.append(String.format(operator.pattern, columnId.asCql(true), buildRightOperand()));
  }

  private String buildRightOperand() {
    StringBuilder builder = new StringBuilder();
    builder.append(opening);
    if (key != null) {
      key.appendTo(builder);
      builder.append(':');
    }
    value.appendTo(builder);
    return builder.append(closing).toString();
  }

  @Override
  public boolean isIdempotent() {
    return (key == null || key.isIdempotent()) && value.isIdempotent();
  }

  public CqlIdentifier getColumnId() {
    return columnId;
  }

  public Term getKey() {
    return key;
  }

  public Term getValue() {
    return value;
  }

  public char getOpening() {
    return opening;
  }

  public char getClosing() {
    return closing;
  }
}
