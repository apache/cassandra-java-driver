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
package com.datastax.dse.driver.api.core.metadata.schema;

import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.ScriptBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

/**
 * Specialized aggregate metadata for DSE.
 *
 * <p>It adds support for the DSE-specific {@link #isDeterministic() DETERMINISTIC} keyword.
 */
public interface DseAggregateMetadata extends AggregateMetadata {

  /**
   * Indicates if this aggregate is deterministic. A deterministic aggregate means that given a
   * particular input, the aggregate will always produce the same output.
   *
   * <p>NOTE: For versions of DSE older than 6.0.0, this method will always return false, regardless
   * of the actual function characteristics.
   *
   * @return Whether or not this aggregate is deterministic.
   */
  boolean isDeterministic();

  @NonNull
  @Override
  default String describe(boolean pretty) {
    // Easiest to just copy the OSS describe() method and add in DETERMINISTIC
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
    // add DETERMINISTIC if present
    if (isDeterministic()) {
      builder.newLine().append("DETERMINISTIC");
    }
    return builder.append(";").build();
  }
}
