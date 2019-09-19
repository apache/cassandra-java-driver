/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.predicates;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

public class CqlCollectionTest {

  @Test
  public void should_evaluate_contains() {
    P<Collection<String>> contains = CqlCollection.contains("foo");
    assertThat(contains.test(new HashSet<>())).isFalse();
    assertThat(contains.test(new ArrayList<>())).isFalse();
    assertThat(contains.test(Sets.newHashSet("foo"))).isTrue();
    assertThat(contains.test(Lists.newArrayList("foo"))).isTrue();
    assertThat(contains.test(Sets.newHashSet("bar"))).isFalse();
    assertThat(contains.test(Lists.newArrayList("bar"))).isFalse();
    assertThatThrownBy(() -> contains.test(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CqlCollection.contains(null).test(Sets.newHashSet("foo")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_evaluate_containsKey() {
    P<Map<String, ?>> containsKey = CqlCollection.containsKey("foo");
    assertThat(containsKey.test(new HashMap<>())).isFalse();
    assertThat(containsKey.test(new LinkedHashMap<>())).isFalse();
    assertThat(containsKey.test(ImmutableMap.of("foo", "bar"))).isTrue();
    assertThat(containsKey.test(ImmutableMap.of("bar", "foo"))).isFalse();
    assertThatThrownBy(() -> containsKey.test(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CqlCollection.containsKey(null).test(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_evaluate_containsValue() {
    P<Map<?, String>> containsValue = CqlCollection.containsValue("foo");
    assertThat(containsValue.test(new HashMap<>())).isFalse();
    assertThat(containsValue.test(new LinkedHashMap<>())).isFalse();
    assertThat(containsValue.test(ImmutableMap.of("bar", "foo"))).isTrue();
    assertThat(containsValue.test(ImmutableMap.of("foo", "bar"))).isFalse();
    assertThatThrownBy(() -> containsValue.test(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CqlCollection.containsValue(null).test(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void should_evaluate_entryEq() {
    P<Map<String, String>> entryEq = CqlCollection.entryEq("foo", "bar");
    assertThat(entryEq.test(new HashMap<>())).isFalse();
    assertThat(entryEq.test(new LinkedHashMap<>())).isFalse();
    assertThat(entryEq.test(ImmutableMap.of("foo", "bar"))).isTrue();
    assertThat(entryEq.test(ImmutableMap.of("bar", "foo"))).isFalse();
    assertThatThrownBy(() -> entryEq.test(null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CqlCollection.entryEq(null, "foo").test(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> CqlCollection.entryEq("foo", null).test(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
