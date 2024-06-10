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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseFunctionMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultFunctionMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseFunctionMetadata extends DefaultFunctionMetadata
    implements DseFunctionMetadata {

  @Nullable private final Boolean deterministic;
  @Nullable private final Monotonicity monotonicity;
  @NonNull private final List<CqlIdentifier> monotonicArgumentNames;

  public DefaultDseFunctionMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull FunctionSignature signature,
      @NonNull List<CqlIdentifier> parameterNames,
      @NonNull String body,
      boolean calledOnNullInput,
      @NonNull String language,
      @NonNull DataType returnType,
      @Nullable Boolean deterministic,
      @Nullable Boolean monotonic,
      @NonNull List<CqlIdentifier> monotonicArgumentNames) {
    super(keyspace, signature, parameterNames, body, calledOnNullInput, language, returnType);
    // set DSE extension attributes
    this.deterministic = deterministic;
    this.monotonicity =
        monotonic == null
            ? null
            : monotonic
                ? Monotonicity.FULLY_MONOTONIC
                : monotonicArgumentNames.isEmpty()
                    ? Monotonicity.NOT_MONOTONIC
                    : Monotonicity.PARTIALLY_MONOTONIC;
    this.monotonicArgumentNames = ImmutableList.copyOf(monotonicArgumentNames);
  }

  @Override
  @Deprecated
  public boolean isDeterministic() {
    return deterministic != null && deterministic;
  }

  @Override
  public Optional<Boolean> getDeterministic() {
    return Optional.ofNullable(deterministic);
  }

  @Override
  @Deprecated
  public boolean isMonotonic() {
    return monotonicity == Monotonicity.FULLY_MONOTONIC;
  }

  @Override
  public Optional<Monotonicity> getMonotonicity() {
    return Optional.ofNullable(monotonicity);
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getMonotonicArgumentNames() {
    return this.monotonicArgumentNames;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseFunctionMetadata) {
      DseFunctionMetadata that = (DseFunctionMetadata) other;
      return Objects.equals(this.getKeyspace(), that.getKeyspace())
          && Objects.equals(this.getSignature(), that.getSignature())
          && Objects.equals(this.getParameterNames(), that.getParameterNames())
          && Objects.equals(this.getBody(), that.getBody())
          && this.isCalledOnNullInput() == that.isCalledOnNullInput()
          && Objects.equals(this.getLanguage(), that.getLanguage())
          && Objects.equals(this.getReturnType(), that.getReturnType())
          && Objects.equals(this.deterministic, that.getDeterministic().orElse(null))
          && this.monotonicity == that.getMonotonicity().orElse(null)
          && Objects.equals(this.monotonicArgumentNames, that.getMonotonicArgumentNames());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getKeyspace(),
        getSignature(),
        getParameterNames(),
        getBody(),
        isCalledOnNullInput(),
        getLanguage(),
        getReturnType(),
        deterministic,
        monotonicity,
        monotonicArgumentNames);
  }

  @Override
  public String toString() {
    return "Function Name: "
        + this.getSignature().getName().asCql(false)
        + ", Keyspace: "
        + this.getKeyspace().asCql(false)
        + ", Language: "
        + this.getLanguage()
        + ", Return Type: "
        + getReturnType().asCql(false, false)
        + ", Deterministic: "
        + this.deterministic
        + ", Monotonicity: "
        + this.monotonicity
        + ", Monotonic On: "
        + (this.monotonicArgumentNames.isEmpty() ? "" : this.monotonicArgumentNames.get(0));
  }
}
