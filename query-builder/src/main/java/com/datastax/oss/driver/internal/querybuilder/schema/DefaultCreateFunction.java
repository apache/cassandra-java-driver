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
package com.datastax.oss.driver.internal.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionWithLanguage;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionWithNullOption;
import com.datastax.oss.driver.api.querybuilder.schema.CreateFunctionWithType;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateFunction
    implements CreateFunctionStart,
        CreateFunctionWithNullOption,
        CreateFunctionWithType,
        CreateFunctionWithLanguage,
        CreateFunctionEnd {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier functionName;
  private boolean orReplace;
  private boolean ifNotExists;
  private final ImmutableMap<CqlIdentifier, DataType> parameters;
  private boolean returnsNullOnNull;
  private final DataType returnType;
  private final String language;
  private final String functionBody;

  public DefaultCreateFunction(@NonNull CqlIdentifier functionName) {
    this(null, functionName);
  }

  public DefaultCreateFunction(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    this(keyspace, functionName, false, false, ImmutableMap.of(), false, null, null, null);
  }

  public DefaultCreateFunction(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier functionName,
      boolean orReplace,
      boolean ifNotExists,
      @NonNull ImmutableMap<CqlIdentifier, DataType> parameters,
      boolean returnsNullOnNull,
      @Nullable DataType returns,
      @Nullable String language,
      @Nullable String functionBody) {
    this.keyspace = keyspace;
    this.functionName = functionName;
    this.orReplace = orReplace;
    this.ifNotExists = ifNotExists;
    this.parameters = parameters;
    this.returnsNullOnNull = returnsNullOnNull;
    this.returnType = returns;
    this.language = language;
    this.functionBody = functionBody;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("CREATE ");
    if (orReplace) {
      builder.append("OR REPLACE ");
    }
    builder.append("FUNCTION ");

    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }
    CqlHelper.qualify(keyspace, functionName, builder);

    builder.append(" (");

    boolean first = true;
    for (Map.Entry<CqlIdentifier, DataType> param : parameters.entrySet()) {
      if (first) {
        first = false;
      } else {
        builder.append(',');
      }
      builder
          .append(param.getKey().asCql(true))
          .append(' ')
          .append(param.getValue().asCql(false, true));
    }
    builder.append(')');
    if (returnsNullOnNull) {
      builder.append(" RETURNS NULL");
    } else {
      builder.append(" CALLED");
    }

    builder.append(" ON NULL INPUT");

    if (returnType == null) {
      // return type has not been provided yet.
      return builder.toString();
    }

    builder.append(" RETURNS ");
    builder.append(returnType.asCql(false, true));

    if (language == null) {
      // language has not been provided yet.
      return builder.toString();
    }

    builder.append(" LANGUAGE ");
    builder.append(language);

    if (functionBody == null) {
      // body has not been provided yet.
      return builder.toString();
    }

    builder.append(" AS ");
    builder.append(functionBody);
    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @NonNull
  @Override
  public CreateFunctionEnd as(@NonNull String functionBody) {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionWithLanguage withLanguage(@NonNull String language) {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionWithType returnsType(@NonNull DataType returnType) {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionStart ifNotExists() {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        true,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionStart orReplace() {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        true,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionStart withParameter(
      @NonNull CqlIdentifier paramName, @NonNull DataType paramType) {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        ImmutableCollections.append(parameters, paramName, paramType),
        returnsNullOnNull,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionWithNullOption returnsNullOnNull() {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        true,
        returnType,
        language,
        functionBody);
  }

  @NonNull
  @Override
  public CreateFunctionWithNullOption calledOnNull() {
    return new DefaultCreateFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        false,
        returnType,
        language,
        functionBody);
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getFunction() {
    return functionName;
  }

  public boolean isOrReplace() {
    return orReplace;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, DataType> getParameters() {
    return parameters;
  }

  public boolean isReturnsNullOnNull() {
    return returnsNullOnNull;
  }

  @Nullable
  public DataType getReturnType() {
    return returnType;
  }

  @Nullable
  public String getLanguage() {
    return language;
  }

  @Nullable
  public String getFunctionBody() {
    return functionBody;
  }
}
