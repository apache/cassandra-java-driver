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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionEnd;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionWithLanguage;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionWithNullOption;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseFunctionWithType;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

/**
 * Implements DSE extended interfaces for creating functions. This class provides the same
 * functionality as the Cassandra OSS {@link
 * com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateFunction} implementation, with
 * the additional DSE specific extended functionality (DETERMINISTIC and MONOTONIC keywords).
 */
@Immutable
public class DefaultCreateDseFunction
    implements CreateDseFunctionEnd,
        CreateDseFunctionStart,
        CreateDseFunctionWithLanguage,
        CreateDseFunctionWithNullOption,
        CreateDseFunctionWithType {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier functionName;
  private boolean orReplace;
  private boolean ifNotExists;
  private final ImmutableMap<CqlIdentifier, DataType> parameters;
  private boolean returnsNullOnNull;
  private final DataType returnType;
  private final String language;
  private final String functionBody;
  private final boolean deterministic;
  private final boolean globallyMonotonic;
  private final CqlIdentifier monotonicOn;

  public DefaultCreateDseFunction(CqlIdentifier functionName) {
    this(null, functionName);
  }

  public DefaultCreateDseFunction(CqlIdentifier keyspace, CqlIdentifier functionName) {
    this(
        keyspace,
        functionName,
        false,
        false,
        ImmutableMap.of(),
        false,
        null,
        null,
        null,
        false,
        false,
        null);
  }

  public DefaultCreateDseFunction(
      CqlIdentifier keyspace,
      CqlIdentifier functionName,
      boolean orReplace,
      boolean ifNotExists,
      ImmutableMap<CqlIdentifier, DataType> parameters,
      boolean returnsNullOnNull,
      DataType returns,
      String language,
      String functionBody,
      boolean deterministic,
      boolean globallyMonotonic,
      CqlIdentifier monotonicOn) {
    this.keyspace = keyspace;
    this.functionName = functionName;
    this.orReplace = orReplace;
    this.ifNotExists = ifNotExists;
    this.parameters = parameters;
    this.returnsNullOnNull = returnsNullOnNull;
    this.returnType = returns;
    this.language = language;
    this.functionBody = functionBody;
    this.deterministic = deterministic;
    this.globallyMonotonic = globallyMonotonic;
    this.monotonicOn = monotonicOn;
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

    // deterministic
    if (deterministic) {
      builder.append(" DETERMINISTIC");
    }

    // monotonic
    if (globallyMonotonic) {
      builder.append(" MONOTONIC");
    } else if (monotonicOn != null) {
      builder.append(" MONOTONIC ON ").append(monotonicOn.asCql(true));
    }

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
  public CreateDseFunctionEnd as(@NonNull String functionBody) {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithLanguage withLanguage(@NonNull String language) {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionStart ifNotExists() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        true,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionStart orReplace() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        true,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionStart withParameter(
      @NonNull CqlIdentifier paramName, @NonNull DataType paramType) {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        ImmutableCollections.append(parameters, paramName, paramType),
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithNullOption returnsNullOnNull() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        true,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithNullOption calledOnNull() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        false,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithType deterministic() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        true,
        globallyMonotonic,
        monotonicOn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithType monotonic() {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        true,
        null);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithType monotonicOn(@NonNull CqlIdentifier monotonicColumn) {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        false,
        monotonicColumn);
  }

  @NonNull
  @Override
  public CreateDseFunctionWithType returnsType(@NonNull DataType returnType) {
    return new DefaultCreateDseFunction(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        returnsNullOnNull,
        returnType,
        language,
        functionBody,
        deterministic,
        globallyMonotonic,
        monotonicOn);
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

  public boolean isDeterministic() {
    return deterministic;
  }

  public boolean isGloballyMonotonic() {
    return globallyMonotonic;
  }

  @Nullable
  public CqlIdentifier getMonotonicOn() {
    return monotonicOn;
  }
}
