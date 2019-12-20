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
package com.datastax.dse.driver.api.core.graph.predicates;

import com.datastax.dse.driver.internal.core.graph.CqlCollectionPredicate;
import java.util.Collection;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;

/**
 * Predicates that can be used on CQL collections (lists, sets and maps).
 *
 * <p>Note: CQL collection predicates are only available when using the binary subprotocol.
 */
public class CqlCollection {

  /**
   * Checks if the target collection contains the given value.
   *
   * @param value the value to look for; cannot be {@code null}.
   * @return a predicate to apply in a {@link
   *     org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal}.
   */
  @SuppressWarnings("unchecked")
  public static <C extends Collection<V>, V> P<C> contains(V value) {
    return new P(CqlCollectionPredicate.contains, value);
  }

  /**
   * Checks if the target map contains the given key.
   *
   * @param key the key to look for; cannot be {@code null}.
   * @return a predicate to apply in a {@link
   *     org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal}.
   */
  @SuppressWarnings("unchecked")
  public static <M extends Map<K, ?>, K> P<M> containsKey(K key) {
    return new P(CqlCollectionPredicate.containsKey, key);
  }

  /**
   * Checks if the target map contains the given value.
   *
   * @param value the value to look for; cannot be {@code null}.
   * @return a predicate to apply in a {@link
   *     org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal}.
   */
  @SuppressWarnings("unchecked")
  public static <M extends Map<?, V>, V> P<M> containsValue(V value) {
    return new P(CqlCollectionPredicate.containsValue, value);
  }

  /**
   * Checks if the target map contains the given entry.
   *
   * @param key the key to look for; cannot be {@code null}.
   * @param value the value to look for; cannot be {@code null}.
   * @return a predicate to apply in a {@link
   *     org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal}.
   */
  @SuppressWarnings("unchecked")
  public static <M extends Map<K, V>, K, V> P<M> entryEq(K key, V value) {
    return new P(CqlCollectionPredicate.entryEq, new Pair<>(key, value));
  }
}
