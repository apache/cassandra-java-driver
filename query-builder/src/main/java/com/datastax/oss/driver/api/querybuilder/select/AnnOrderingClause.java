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
package com.datastax.oss.driver.api.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Concrete implementation of {@link OrderingClause} which supports ordering by
 * the adjacent nearest-neighbor (ANN) calculation.  This usage is primarily used
 * for vector calculations.
 */
public class AnnOrderingClause extends OrderingClause {

  private final CqlIdentifier identifier;
  private final CqlVector<?> vector;

  AnnOrderingClause(CqlIdentifier identifier, CqlVector<?> vector) {

    this.identifier = identifier;
    this.vector = vector;
  }

  public static AnnOrderingClause create(CqlIdentifier identifier, CqlVector<?> vector) {
    return new AnnOrderingClause(identifier, vector);
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {
    builder.append(" ORDER BY ").append(this.identifier.asCql(true)).append(" ANN OF ");
    QueryBuilder.literal(this.vector).appendTo(builder);
  }
}
