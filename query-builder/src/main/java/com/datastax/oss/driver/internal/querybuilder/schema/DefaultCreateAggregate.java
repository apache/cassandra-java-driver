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
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateEnd;
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateAggregateStateFunc;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateAggregate
    implements CreateAggregateStart, CreateAggregateStateFunc, CreateAggregateEnd {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier functionName;
  private boolean orReplace;
  private boolean ifNotExists;
  private final ImmutableList<DataType> parameters;
  private final CqlIdentifier sFunc;
  private final DataType sType;
  private final CqlIdentifier finalFunc;
  private final Term term;

  public DefaultCreateAggregate(@NonNull CqlIdentifier functionName) {
    this(null, functionName);
  }

  public DefaultCreateAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    this(keyspace, functionName, false, false, ImmutableList.of(), null, null, null, null);
  }

  public DefaultCreateAggregate(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier functionName,
      boolean orReplace,
      boolean ifNotExists,
      @NonNull ImmutableList<DataType> parameters,
      @Nullable CqlIdentifier sFunc,
      @Nullable DataType sType,
      @Nullable CqlIdentifier finalFunc,
      @Nullable Term term) {
    this.keyspace = keyspace;
    this.functionName = functionName;
    this.orReplace = orReplace;
    this.ifNotExists = ifNotExists;
    this.parameters = parameters;
    this.sFunc = sFunc;
    this.sType = sType;
    this.finalFunc = finalFunc;
    this.term = term;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("CREATE ");
    if (orReplace) {
      builder.append("OR REPLACE ");
    }
    builder.append("AGGREGATE ");

    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }
    CqlHelper.qualify(keyspace, functionName, builder);

    builder.append(" (");
    boolean first = true;
    for (DataType param : parameters) {
      if (first) {
        first = false;
      } else {
        builder.append(',');
      }
      builder.append(param.asCql(false, true));
    }
    builder.append(')');
    if (sFunc != null) {
      builder.append(" SFUNC ");
      builder.append(sFunc.asCql(true));
    }
    if (sType != null) {
      builder.append(" STYPE ");
      builder.append(sType.asCql(false, true));
    }
    if (finalFunc != null) {
      builder.append(" FINALFUNC ");
      builder.append(finalFunc.asCql(true));
    }
    if (term != null) {
      builder.append(" INITCOND ");
      term.appendTo(builder);
    }
    return builder.toString();
  }

  @NonNull
  @Override
  public CreateAggregateEnd withInitCond(@NonNull Term term) {
    return new DefaultCreateAggregate(
        keyspace, functionName, orReplace, ifNotExists, parameters, sFunc, sType, finalFunc, term);
  }

  @NonNull
  @Override
  public CreateAggregateStart ifNotExists() {
    return new DefaultCreateAggregate(
        keyspace, functionName, orReplace, true, parameters, sFunc, sType, finalFunc, term);
  }

  @NonNull
  @Override
  public CreateAggregateStart orReplace() {
    return new DefaultCreateAggregate(
        keyspace, functionName, true, ifNotExists, parameters, sFunc, sType, finalFunc, term);
  }

  @NonNull
  @Override
  public CreateAggregateStart withParameter(@NonNull DataType paramType) {
    return new DefaultCreateAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        ImmutableCollections.append(parameters, paramType),
        sFunc,
        sType,
        finalFunc,
        term);
  }

  @NonNull
  @Override
  public CreateAggregateStateFunc withSFunc(@NonNull CqlIdentifier sFunc) {
    return new DefaultCreateAggregate(
        keyspace, functionName, orReplace, ifNotExists, parameters, sFunc, sType, finalFunc, term);
  }

  @NonNull
  @Override
  public CreateAggregateEnd withSType(@NonNull DataType sType) {
    return new DefaultCreateAggregate(
        keyspace, functionName, orReplace, ifNotExists, parameters, sFunc, sType, finalFunc, term);
  }

  @NonNull
  @Override
  public CreateAggregateEnd withFinalFunc(@NonNull CqlIdentifier finalFunc) {
    return new DefaultCreateAggregate(
        keyspace, functionName, orReplace, ifNotExists, parameters, sFunc, sType, finalFunc, term);
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getFunctionName() {
    return functionName;
  }

  public boolean isOrReplace() {
    return orReplace;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @NonNull
  public ImmutableList<DataType> getParameters() {
    return parameters;
  }

  @Nullable
  public CqlIdentifier getsFunc() {
    return sFunc;
  }

  @Nullable
  public DataType getsType() {
    return sType;
  }

  @Nullable
  public CqlIdentifier getFinalFunc() {
    return finalFunc;
  }

  @Nullable
  public Term getTerm() {
    return term;
  }
}
