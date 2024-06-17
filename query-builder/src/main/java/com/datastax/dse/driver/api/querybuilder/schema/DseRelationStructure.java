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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

public interface DseRelationStructure<SelfT extends DseRelationStructure<SelfT>>
    extends DseRelationOptions<SelfT> {

  /**
   * Adds the provided CLUSTERING ORDER.
   *
   * <p>They will be appended in the iteration order of the provided map. If an ordering was already
   * defined for a given identifier, it will be removed and the new ordering will appear in its
   * position in the provided map.
   */
  @NonNull
  SelfT withClusteringOrderByIds(@NonNull Map<CqlIdentifier, ClusteringOrder> orderings);

  /**
   * Shortcut for {@link #withClusteringOrderByIds(Map)} with the columns specified as
   * case-insensitive names. They will be wrapped with {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>Note that it's possible for two different case-sensitive names to resolve to the same
   * identifier, for example "foo" and "Foo"; if this happens, a runtime exception will be thrown.
   */
  @NonNull
  default SelfT withClusteringOrder(@NonNull Map<String, ClusteringOrder> orderings) {
    return withClusteringOrderByIds(CqlIdentifiers.wrapKeys(orderings));
  }

  /**
   * Adds the provided clustering order.
   *
   * <p>If clustering order was already defined for this identifier, it will be removed and the new
   * clause will be appended at the end of the current clustering order.
   */
  @NonNull
  SelfT withClusteringOrder(@NonNull CqlIdentifier columnName, @NonNull ClusteringOrder order);

  /**
   * Shortcut for {@link #withClusteringOrder(CqlIdentifier, ClusteringOrder)
   * withClusteringOrder(CqlIdentifier.fromCql(columnName), order)}.
   */
  @NonNull
  default SelfT withClusteringOrder(@NonNull String columnName, @NonNull ClusteringOrder order) {
    return withClusteringOrder(CqlIdentifier.fromCql(columnName), order);
  }
}
