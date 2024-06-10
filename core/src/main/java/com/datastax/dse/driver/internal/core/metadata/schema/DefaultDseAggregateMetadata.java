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

import com.datastax.dse.driver.api.core.metadata.schema.DseAggregateMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultAggregateMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Objects;
import java.util.Optional;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseAggregateMetadata extends DefaultAggregateMetadata
    implements DseAggregateMetadata {

  @Nullable private final Boolean deterministic;

  public DefaultDseAggregateMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull FunctionSignature signature,
      @Nullable FunctionSignature finalFuncSignature,
      @Nullable Object initCond,
      @NonNull DataType returnType,
      @NonNull FunctionSignature stateFuncSignature,
      @NonNull DataType stateType,
      @NonNull TypeCodec<Object> stateTypeCodec,
      @Nullable Boolean deterministic) {
    super(
        keyspace,
        signature,
        finalFuncSignature,
        initCond,
        returnType,
        stateFuncSignature,
        stateType,
        stateTypeCodec);
    this.deterministic = deterministic;
  }

  @Override
  @Deprecated
  public boolean isDeterministic() {
    return deterministic != null && deterministic;
  }

  @Override
  @Nullable
  public Optional<Boolean> getDeterministic() {
    return Optional.ofNullable(deterministic);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseAggregateMetadata) {
      DseAggregateMetadata that = (DseAggregateMetadata) other;
      return Objects.equals(this.getKeyspace(), that.getKeyspace())
          && Objects.equals(this.getSignature(), that.getSignature())
          && Objects.equals(
              this.getFinalFuncSignature().orElse(null), that.getFinalFuncSignature().orElse(null))
          && Objects.equals(this.getInitCond().orElse(null), that.getInitCond().orElse(null))
          && Objects.equals(this.getReturnType(), that.getReturnType())
          && Objects.equals(this.getStateFuncSignature(), that.getStateFuncSignature())
          && Objects.equals(this.getStateType(), that.getStateType())
          && Objects.equals(this.deterministic, that.getDeterministic().orElse(null));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getKeyspace(),
        getSignature(),
        getFinalFuncSignature(),
        getInitCond(),
        getReturnType(),
        getStateFuncSignature(),
        getStateType(),
        deterministic);
  }

  @Override
  public String toString() {
    return "Aggregate Name: "
        + getSignature().getName().asCql(false)
        + ", Keyspace: "
        + getKeyspace().asCql(false)
        + ", Return Type: "
        + getReturnType().asCql(false, false)
        + ", Deterministic: "
        + deterministic;
  }
}
