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
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public interface RelationStructure<SelfT extends RelationStructure<SelfT>>
    extends RelationOptions<SelfT> {

  /**
   * Adds the provided CLUSTERING ORDER.
   *
   * <p>They will be appended in the iteration order of the provided map. If an ordering was already
   * defined for a given identifier, it will be removed and the new ordering will appear in its
   * position in the provided map.
   */
  SelfT withClusteringOrderByIds(Map<CqlIdentifier, ClusteringOrder> orderings);

  /**
   * Shortcut for {@link #withClusteringOrderByIds(Map)} with the columns specified as
   * case-insensitive names. They will be wrapped with {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>Note that it's possible for two different case-sensitive names to resolve to the same
   * identifier, for example "foo" and "Foo"; if this happens, a runtime exception will be thrown.
   */
  default SelfT withClusteringOrder(Map<String, ClusteringOrder> orderings) {
    ImmutableMap.Builder<CqlIdentifier, ClusteringOrder> builder = ImmutableMap.builder();
    for (Map.Entry<String, ClusteringOrder> entry : orderings.entrySet()) {
      builder.put(CqlIdentifier.fromCql(entry.getKey()), entry.getValue());
    }
    // build() throws if there are duplicate keys
    return withClusteringOrderByIds(builder.build());
  }

  /**
   * Adds the provided clustering order.
   *
   * <p>If clustering order was already defined for this identifier, it will be removed and the new
   * clause will be appended at the end of the current clustering order.
   */
  SelfT withClusteringOrder(CqlIdentifier columnName, ClusteringOrder order);

  /**
   * Shortcut for {@link #withClusteringOrder(CqlIdentifier, ClusteringOrder)
   * withClusteringOrder(CqlIdentifier.fromCql(columnName), order)}.
   */
  default SelfT withClusteringOrder(String columnName, ClusteringOrder order) {
    return withClusteringOrder(CqlIdentifier.fromCql(columnName), order);
  }
}
