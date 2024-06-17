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
package com.datastax.oss.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

/** A CQL aggregate in the schema metadata. */
public interface AggregateMetadata extends Describable {

  @NonNull
  CqlIdentifier getKeyspace();

  @NonNull
  FunctionSignature getSignature();

  /**
   * The signature of the final function of this aggregate, or empty if there is none.
   *
   * <p>This is the function specified with {@code FINALFUNC} in the {@code CREATE AGGREGATE...}
   * statement. It transforms the final value after the aggregation is complete.
   */
  @NonNull
  Optional<FunctionSignature> getFinalFuncSignature();

  /**
   * The initial state value of this aggregate, or {@code null} if there is none.
   *
   * <p>This is the value specified with {@code INITCOND} in the {@code CREATE AGGREGATE...}
   * statement. It's passed to the initial invocation of the state function (if that function does
   * not accept null arguments).
   *
   * <p>The actual type of the returned object depends on the aggregate's {@link #getStateType()
   * state type} and on the {@link TypeCodec codec} used to {@link TypeCodec#parse(String) parse}
   * the {@code INITCOND} literal.
   *
   * <p>If, for some reason, the {@code INITCOND} literal cannot be parsed, a warning will be logged
   * and the returned object will be the original {@code INITCOND} literal in its textual,
   * non-parsed form.
   *
   * @return the initial state, or empty if there is none.
   */
  @NonNull
  Optional<Object> getInitCond();

  /**
   * The return type of this aggregate.
   *
   * <p>This is the final type of the value computed by this aggregate; in other words, the return
   * type of the final function if it is defined, or the state type otherwise.
   */
  @NonNull
  DataType getReturnType();

  /**
   * The signature of the state function of this aggregate.
   *
   * <p>This is the function specified with {@code SFUNC} in the {@code CREATE AGGREGATE...}
   * statement. It aggregates the current state with each row to produce a new state.
   */
  @NonNull
  FunctionSignature getStateFuncSignature();

  /**
   * The state type of this aggregate.
   *
   * <p>This is the type specified with {@code STYPE} in the {@code CREATE AGGREGATE...} statement.
   * It defines the type of the value that is accumulated as the aggregate iterates through the
   * rows.
   */
  @NonNull
  DataType getStateType();

  @NonNull
  @Override
  default String describeWithChildren(boolean pretty) {
    // An aggregate has no children
    return describe(pretty);
  }

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    builder
        .append("CREATE AGGREGATE ")
        .append(getKeyspace())
        .append(".")
        .append(getSignature().getName())
        .append("(");
    boolean first = true;
    for (int i = 0; i < getSignature().getParameterTypes().size(); i++) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      DataType type = getSignature().getParameterTypes().get(i);
      builder.append(type.asCql(false, pretty));
    }
    builder
        .increaseIndent()
        .append(")")
        .newLine()
        .append("SFUNC ")
        .append(getStateFuncSignature().getName())
        .newLine()
        .append("STYPE ")
        .append(getStateType().asCql(false, pretty));

    if (getFinalFuncSignature().isPresent()) {
      builder.newLine().append("FINALFUNC ").append(getFinalFuncSignature().get().getName());
    }
    if (getInitCond().isPresent()) {
      Optional<String> formatInitCond = formatInitCond();
      assert formatInitCond.isPresent();
      builder.newLine().append("INITCOND ").append(formatInitCond.get());
    }
    return builder.append(";").build();
  }

  /**
   * Formats the {@linkplain #getInitCond() initial state value} for inclusion in a CQL statement.
   */
  @NonNull
  Optional<String> formatInitCond();
}
