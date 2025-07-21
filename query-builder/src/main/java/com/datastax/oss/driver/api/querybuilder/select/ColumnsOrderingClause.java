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
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/**
 * Concrete implementation of {@link OrderingClause} which supports ordering by
 * specified columns.  This usages is the default ORDER BY syntax for Apache Cassandra.
 */
public class ColumnsOrderingClause extends OrderingClause {

  private final ImmutableMap<CqlIdentifier, ClusteringOrder> orderings;

  ColumnsOrderingClause(ImmutableMap<CqlIdentifier, ClusteringOrder> orderings) {

    this.orderings = orderings;
  }

  public static ColumnsOrderingClause create() {
    return new ColumnsOrderingClause(ImmutableMap.of());
  }

  public ColumnsOrderingClause add(
      @NonNull CqlIdentifier identifier, @NonNull ClusteringOrder order) {
    return new ColumnsOrderingClause(
        ImmutableCollections.append(this.orderings, identifier, order));
  }

  public ColumnsOrderingClause add(@NonNull Map<CqlIdentifier, ClusteringOrder> orderMap) {
    return new ColumnsOrderingClause(ImmutableCollections.concat(this.orderings, orderMap));
  }

  @Override
  public void appendTo(@NonNull StringBuilder builder) {

    boolean first = true;
    for (Map.Entry<CqlIdentifier, ClusteringOrder> entry : orderings.entrySet()) {
      if (first) {
        builder.append(" ORDER BY ");
        first = false;
      } else {
        builder.append(",");
      }
      builder.append(entry.getKey().asCql(true)).append(" ").append(entry.getValue().name());
    }
  }
}
