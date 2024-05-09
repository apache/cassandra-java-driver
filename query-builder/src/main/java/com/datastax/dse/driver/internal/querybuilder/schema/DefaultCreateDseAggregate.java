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
package com.datastax.dse.driver.internal.querybuilder.schema;

import com.datastax.dse.driver.api.querybuilder.schema.CreateDseAggregateEnd;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseAggregateStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseAggregateStateFunc;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import net.jcip.annotations.Immutable;

/**
 * Implements DSE extended interfaces for creating aggregates. This class provides the same
 * functionality as the Cassandra OSS {@link
 * com.datastax.oss.driver.internal.querybuilder.schema.DefaultCreateAggregate} implementation, with
 * the additional DSE specific extended functionality (DETERMINISTIC keyword).
 */
@Immutable
public class DefaultCreateDseAggregate
    implements CreateDseAggregateEnd, CreateDseAggregateStart, CreateDseAggregateStateFunc {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier functionName;
  private boolean orReplace;
  private boolean ifNotExists;
  private final ImmutableList<DataType> parameters;
  private final CqlIdentifier sFunc;
  private final DataType sType;
  private final CqlIdentifier finalFunc;
  private final Term term;
  private final boolean deterministic;

  public DefaultCreateDseAggregate(@NonNull CqlIdentifier functionName) {
    this(null, functionName);
  }

  public DefaultCreateDseAggregate(
      @Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier functionName) {
    this(keyspace, functionName, false, false, ImmutableList.of(), null, null, null, null, false);
  }

  public DefaultCreateDseAggregate(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier functionName,
      boolean orReplace,
      boolean ifNotExists,
      @NonNull ImmutableList<DataType> parameters,
      @Nullable CqlIdentifier sFunc,
      @Nullable DataType sType,
      @Nullable CqlIdentifier finalFunc,
      @Nullable Term term,
      boolean deterministic) {
    this.keyspace = keyspace;
    this.functionName = functionName;
    this.orReplace = orReplace;
    this.ifNotExists = ifNotExists;
    this.parameters = parameters;
    this.sFunc = sFunc;
    this.sType = sType;
    this.finalFunc = finalFunc;
    this.term = term;
    this.deterministic = deterministic;
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
    // deterministic
    if (deterministic) {
      builder.append(" DETERMINISTIC");
    }
    return builder.toString();
  }

  @NonNull
  @Override
  public CreateDseAggregateEnd withInitCond(@NonNull Term term) {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateStart ifNotExists() {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        true,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateStart orReplace() {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        true,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateStart withParameter(@NonNull DataType paramType) {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        ImmutableCollections.append(parameters, paramType),
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateStateFunc withSFunc(@NonNull CqlIdentifier sFunc) {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateEnd withSType(@NonNull DataType sType) {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateEnd withFinalFunc(@NonNull CqlIdentifier finalFunc) {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        deterministic);
  }

  @NonNull
  @Override
  public CreateDseAggregateEnd deterministic() {
    return new DefaultCreateDseAggregate(
        keyspace,
        functionName,
        orReplace,
        ifNotExists,
        parameters,
        sFunc,
        sType,
        finalFunc,
        term,
        true);
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

  public boolean isDeterministic() {
    return deterministic;
  }
}
