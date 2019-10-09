/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class TinkerVertexPropertyAssert<T>
    extends TinkerElementAssert<TinkerVertexPropertyAssert<T>, VertexProperty<T>> {

  public TinkerVertexPropertyAssert(VertexProperty<T> actual) {
    super(actual, TinkerVertexPropertyAssert.class);
  }

  public TinkerVertexPropertyAssert<T> hasKey(String key) {
    assertThat(actual.key()).isEqualTo(key);
    return this;
  }

  public TinkerVertexPropertyAssert<T> hasParent(Element parent) {
    assertThat(actual.element()).isEqualTo(parent);
    return this;
  }

  public TinkerVertexPropertyAssert<T> hasValue(T value) {
    assertThat(actual.value()).isEqualTo(value);
    return this;
  }
}
