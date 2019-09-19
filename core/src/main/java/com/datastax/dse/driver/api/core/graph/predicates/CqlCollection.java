/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
