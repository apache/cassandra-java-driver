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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Optional;

/**
 * Specialized function metadata for DSE.
 *
 * <p>It adds support for the DSE-specific {@link #getDeterministic() DETERMINISTIC} and {@link
 * #getMonotonicity() MONOTONIC} keywords.
 */
public interface DseFunctionMetadata extends FunctionMetadata {

  /** The monotonicity of a function. */
  enum Monotonicity {

    /**
     * Indicates that the function is fully monotonic on all of its arguments. This means that it is
     * either entirely non-increasing or non-decreasing. Full monotonicity is required to use the
     * function in a GROUP BY clause.
     */
    FULLY_MONOTONIC,

    /**
     * Indicates that the function is partially monotonic, meaning that partial application over
     * some of the its arguments is monotonic. Currently (DSE 6.0.0), CQL only allows partial
     * monotonicity on <em>exactly one argument</em>. This may change in a future CQL version.
     */
    PARTIALLY_MONOTONIC,

    /** Indicates that the function is not monotonic. */
    NOT_MONOTONIC,
  }

  /** @deprecated Use {@link #getDeterministic()} instead. */
  @Deprecated
  boolean isDeterministic();

  /**
   * Indicates if this function is deterministic. A deterministic function means that given a
   * particular input, the function will always produce the same output.
   *
   * <p>This method returns {@linkplain Optional#empty() empty} if this information was not found in
   * the system tables, regardless of the actual function characteristics; this is the case for all
   * versions of DSE older than 6.0.0.
   *
   * @return Whether or not this function is deterministic; or {@linkplain Optional#empty() empty}
   *     if such information is not available in the system tables.
   */
  default Optional<Boolean> getDeterministic() {
    return Optional.of(isDeterministic());
  }

  /** @deprecated use {@link #getMonotonicity()} instead. */
  @Deprecated
  boolean isMonotonic();

  /**
   * Returns this function's {@link Monotonicity}.
   *
   * <p>A function can be either:
   *
   * <ul>
   *   <li>fully monotonic. In that case, this method returns {@link Monotonicity#FULLY_MONOTONIC},
   *       and {@link #getMonotonicArgumentNames()} returns all the arguments;
   *   <li>partially monotonic, meaning that partial application over some of the arguments is
   *       monotonic. Currently (DSE 6.0.0), CQL only allows partial monotonicity on <em>exactly one
   *       argument</em>. This may change in a future CQL version. In that case, this method returns
   *       {@link Monotonicity#PARTIALLY_MONOTONIC}, and {@link #getMonotonicArgumentNames()}
   *       returns a singleton list;
   *   <li>not monotonic. In that case, this method return {@link Monotonicity#NOT_MONOTONIC} and
   *       {@link #getMonotonicArgumentNames()} returns an empty list.
   * </ul>
   *
   * <p>Full monotonicity is required to use the function in a GROUP BY clause.
   *
   * <p>This method returns {@linkplain Optional#empty() empty} if this information was not found in
   * the system tables, regardless of the actual function characteristics; this is the case for all
   * versions of DSE older than 6.0.0.
   *
   * @return this function's {@link Monotonicity}; or {@linkplain Optional#empty() empty} if such
   *     information is not available in the system tables.
   */
  default Optional<Monotonicity> getMonotonicity() {
    return Optional.of(
        isMonotonic()
            ? Monotonicity.FULLY_MONOTONIC
            : getMonotonicArgumentNames().isEmpty()
                ? Monotonicity.NOT_MONOTONIC
                : Monotonicity.PARTIALLY_MONOTONIC);
  }

  /**
   * Returns a list of argument names that are monotonic.
   *
   * <p>See {@link #getMonotonicity()} for explanations on monotonicity, and the possible values
   * returned by this method.
   *
   * <p>NOTE: For versions of DSE older than 6.0.0, this method will always return an empty list,
   * regardless of the actual function characteristics.
   *
   * @return the argument names that the function is monotonic on.
   */
  @NonNull
  List<CqlIdentifier> getMonotonicArgumentNames();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    ScriptBuilder builder = new ScriptBuilder(pretty);
    builder
        .append("CREATE FUNCTION ")
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
      CqlIdentifier name = getParameterNames().get(i);
      builder.append(name).append(" ").append(type.asCql(false, pretty));
    }
    builder
        .append(")")
        .increaseIndent()
        .newLine()
        .append(isCalledOnNullInput() ? "CALLED ON NULL INPUT" : "RETURNS NULL ON NULL INPUT")
        .newLine()
        .append("RETURNS ")
        .append(getReturnType().asCql(false, true))
        .newLine();
    // handle deterministic and monotonic
    if (getDeterministic().orElse(false)) {
      builder.append("DETERMINISTIC").newLine();
    }
    if (getMonotonicity().isPresent()) {
      switch (getMonotonicity().get()) {
        case FULLY_MONOTONIC:
          builder.append("MONOTONIC").newLine();
          break;
        case PARTIALLY_MONOTONIC:
          builder.append("MONOTONIC ON ").append(getMonotonicArgumentNames().get(0)).newLine();
          break;
        default:
          break;
      }
    }
    builder
        .append("LANGUAGE ")
        .append(getLanguage())
        .newLine()
        .append("AS '")
        .append(getBody())
        .append("';");
    return builder.build();
  }
}
