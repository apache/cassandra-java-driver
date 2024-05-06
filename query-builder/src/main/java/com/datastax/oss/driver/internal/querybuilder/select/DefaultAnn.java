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
package com.datastax.oss.driver.internal.querybuilder.select;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.select.Ann;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public class DefaultAnn implements Ann {
  private final CqlIdentifier cqlIdentifier;
  private final CqlVector<Number> vector;

  public DefaultAnn(@NonNull CqlIdentifier cqlIdentifier, @NonNull CqlVector<Number> vector) {
    this.cqlIdentifier = cqlIdentifier;
    this.vector = vector;
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();
    builder.append("ORDER BY ");
    builder.append(this.cqlIdentifier.asCql(true));
    builder.append(" ANN OF ");
    literal(vector).appendTo(builder);
    return builder.toString();
  }

  @NonNull
  @Override
  public SimpleStatement build() {
    return Ann.super.build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Object... values) {
    return Ann.super.build(values);
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
    return Ann.super.build(namedValues);
  }

  @NonNull
  @Override
  public SimpleStatementBuilder builder() {
    return Ann.super.builder();
  }
}
