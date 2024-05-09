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
package com.datastax.oss.driver.api.querybuilder.condition;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.condition.DefaultConditionBuilder;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnComponentLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.ColumnLeftOperand;
import com.datastax.oss.driver.internal.querybuilder.lhs.FieldLeftOperand;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A condition in a {@link ConditionalStatement}.
 *
 * <p>To build instances of this type, use the factory methods, such as {@link #column(String)
 * column}, {@link #field(String, String)} field}, etc.
 *
 * <p>They are used as arguments to the {@link ConditionalStatement#if_(Iterable)} method, for
 * example:
 *
 * <pre>{@code
 * deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker())
 *     .if_(Condition.column("v").isEqualTo(literal(1)))
 * // DELETE FROM foo WHERE k=? IF v=1
 * }</pre>
 *
 * There are also shortcuts in the fluent API when you build a statement, for example:
 *
 * <pre>{@code
 * deleteFrom("foo").whereColumn("k").isEqualTo(bindMarker())
 *     .ifColumn("v").isEqualTo(literal(1))
 * // DELETE FROM foo WHERE k=? IF v=1
 * }</pre>
 */
public interface Condition extends CqlSnippet {

  /** Builds a condition on a column for a conditional statement, as in {@code DELETE... IF k=1}. */
  @NonNull
  static ConditionBuilder<Condition> column(@NonNull CqlIdentifier columnId) {
    return new DefaultConditionBuilder(new ColumnLeftOperand(columnId));
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))}. */
  @NonNull
  static ConditionBuilder<Condition> column(@NonNull String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Builds a condition on a field in a UDT column for a conditional statement, as in {@code
   * DELETE... IF address.street='test'}.
   */
  @NonNull
  static ConditionBuilder<Condition> field(
      @NonNull CqlIdentifier columnId, @NonNull CqlIdentifier fieldId) {
    return new DefaultConditionBuilder(new FieldLeftOperand(columnId, fieldId));
  }

  /**
   * Shortcut for {@link #field(CqlIdentifier, CqlIdentifier)
   * field(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName))}.
   */
  @NonNull
  static ConditionBuilder<Condition> field(@NonNull String columnName, @NonNull String fieldName) {
    return field(CqlIdentifier.fromCql(columnName), CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Builds a condition on an element in a collection column for a conditional statement, as in
   * {@code DELETE... IF m[0]=1}.
   */
  @NonNull
  static ConditionBuilder<Condition> element(@NonNull CqlIdentifier columnId, @NonNull Term index) {
    return new DefaultConditionBuilder(new ColumnComponentLeftOperand(columnId, index));
  }

  /**
   * Shortcut for {@link #element(CqlIdentifier, Term) element(CqlIdentifier.fromCql(columnName),
   * index)}.
   */
  @NonNull
  static ConditionBuilder<Condition> element(@NonNull String columnName, @NonNull Term index) {
    return element(CqlIdentifier.fromCql(columnName), index);
  }
}
