/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.FunctionSignature;
import com.datastax.oss.driver.api.core.type.DataType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Objects;

public class DefaultFunctionMetadata implements FunctionMetadata {

  private final CqlIdentifier keyspace;
  private final FunctionSignature signature;
  private final List<CqlIdentifier> parameterNames;
  private final String body;
  private final boolean calledOnNullInput;
  private final String language;
  private final DataType returnType;

  public DefaultFunctionMetadata(
      CqlIdentifier keyspace,
      FunctionSignature signature,
      List<CqlIdentifier> parameterNames,
      String body,
      boolean calledOnNullInput,
      String language,
      DataType returnType) {
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

  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Override
  public FunctionSignature getSignature() {
    return signature;
  }

  @Override
  public List<CqlIdentifier> getParameterNames() {
    return parameterNames;
  }

  @Override
  public String getBody() {
    return body;
  }

  @Override
  public boolean isCalledOnNullInput() {
    return calledOnNullInput;
  }

  @Override
  public String getLanguage() {
    return language;
  }

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
}
