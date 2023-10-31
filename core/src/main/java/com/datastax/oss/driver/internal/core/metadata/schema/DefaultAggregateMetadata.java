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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.AggregateMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class DefaultAggregateMetadata implements AggregateMetadata, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultAggregateMetadata.class);

  private static final long serialVersionUID = 1;

  @Nonnull private final CqlIdentifier keyspace;
  @Nonnull private final FunctionSignature signature;
  @Nullable private final FunctionSignature finalFuncSignature;
  @Nullable private final Object initCond;
  @Nullable private final String formattedInitCond;
  @Nonnull private final DataType returnType;
  @Nonnull private final FunctionSignature stateFuncSignature;
  @Nonnull private final DataType stateType;

  public DefaultAggregateMetadata(
      @Nonnull CqlIdentifier keyspace,
      @Nonnull FunctionSignature signature,
      @Nullable FunctionSignature finalFuncSignature,
      @Nullable Object initCond,
      @Nonnull DataType returnType,
      @Nonnull FunctionSignature stateFuncSignature,
      @Nonnull DataType stateType,
      @Nonnull TypeCodec<Object> stateTypeCodec) {
    this.keyspace = keyspace;
    this.signature = signature;
    this.finalFuncSignature = finalFuncSignature;
    this.initCond = initCond;
    this.formattedInitCond = computeFormattedInitCond(initCond, stateTypeCodec);
    this.returnType = returnType;
    this.stateFuncSignature = stateFuncSignature;
    this.stateType = stateType;
  }

  @Nonnull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  @Override
  public FunctionSignature getSignature() {
    return signature;
  }

  @Nonnull
  @Override
  public Optional<FunctionSignature> getFinalFuncSignature() {
    return Optional.ofNullable(finalFuncSignature);
  }

  @Nonnull
  @Override
  public Optional<Object> getInitCond() {
    return Optional.ofNullable(initCond);
  }

  @Nonnull
  @Override
  public DataType getReturnType() {
    return returnType;
  }

  @Nonnull
  @Override
  public FunctionSignature getStateFuncSignature() {
    return stateFuncSignature;
  }

  @Nonnull
  @Override
  public DataType getStateType() {
    return stateType;
  }

  @Nonnull
  @Override
  public Optional<String> formatInitCond() {
    return Optional.ofNullable(this.formattedInitCond);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof AggregateMetadata) {
      AggregateMetadata that = (AggregateMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.signature, that.getSignature())
          && Objects.equals(this.finalFuncSignature, that.getFinalFuncSignature().orElse(null))
          && Objects.equals(this.initCond, that.getInitCond().orElse(null))
          && Objects.equals(this.returnType, that.getReturnType())
          && Objects.equals(this.stateFuncSignature, that.getStateFuncSignature())
          && Objects.equals(this.stateType, that.getStateType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keyspace,
        signature,
        finalFuncSignature,
        initCond,
        returnType,
        stateFuncSignature,
        stateType);
  }

  @Override
  public String toString() {
    return "DefaultAggregateMetadata@"
        + Integer.toHexString(hashCode())
        + "("
        + keyspace.asInternal()
        + "."
        + signature
        + ")";
  }

  @Nullable
  private String computeFormattedInitCond(
      @Nullable Object initCond, @Nonnull TypeCodec<Object> stateTypeCodec) {

    if (initCond == null) {
      return null;
    }
    try {
      return stateTypeCodec.format(initCond);
    } catch (Throwable t) {
      LOG.warn(
          String.format(
              "Failed to format INITCOND for %s.%s, using toString instead",
              keyspace.asInternal(), signature.getName().asInternal()));
      return initCond.toString();
    }
  }
}
