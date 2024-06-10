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
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultFunctionMetadata implements FunctionMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final FunctionSignature signature;
  @NonNull private final List<CqlIdentifier> parameterNames;
  @NonNull private final String body;
  private final boolean calledOnNullInput;
  @NonNull private final String language;
  @NonNull private final DataType returnType;

  public DefaultFunctionMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull FunctionSignature signature,
      @NonNull List<CqlIdentifier> parameterNames,
      @NonNull String body,
      boolean calledOnNullInput,
      @NonNull String language,
      @NonNull DataType returnType) {
    Preconditions.checkArgument(
        signature.getParameterTypes().size() == parameterNames.size(),
        "Number of parameter names should match number of types in the signature (got %s and %s)",
        parameterNames.size(),
        signature.getParameterTypes().size());
    this.keyspace = keyspace;
    this.signature = signature;
    this.parameterNames = parameterNames;
    this.body = body;
    this.calledOnNullInput = calledOnNullInput;
    this.language = language;
    this.returnType = returnType;
  }

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public FunctionSignature getSignature() {
    return signature;
  }

  @NonNull
  @Override
  public List<CqlIdentifier> getParameterNames() {
    return parameterNames;
  }

  @NonNull
  @Override
  public String getBody() {
    return body;
  }

  @Override
  public boolean isCalledOnNullInput() {
    return calledOnNullInput;
  }

  @NonNull
  @Override
  public String getLanguage() {
    return language;
  }

  @NonNull
  @Override
  public DataType getReturnType() {
    return returnType;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof FunctionMetadata) {
      FunctionMetadata that = (FunctionMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.signature, that.getSignature())
          && Objects.equals(this.parameterNames, that.getParameterNames())
          && Objects.equals(this.body, that.getBody())
          && this.calledOnNullInput == that.isCalledOnNullInput()
          && Objects.equals(this.language, that.getLanguage())
          && Objects.equals(this.returnType, that.getReturnType());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keyspace, signature, parameterNames, body, calledOnNullInput, language, returnType);
  }

  @Override
  public String toString() {
    return "DefaultFunctionMetadata@"
        + Integer.toHexString(hashCode())
        + "("
        + keyspace.asInternal()
        + "."
        + signature
        + ")";
  }
}
