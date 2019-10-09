/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerVertexAssert extends TinkerElementAssert<TinkerVertexAssert, Vertex> {

  public TinkerVertexAssert(Vertex actual) {
    super(actual, TinkerVertexAssert.class);
  }

  @Override
  public TinkerVertexAssert hasProperty(String propertyName) {
    assertThat(actual.properties(propertyName)).toIterable().isNotEmpty();
    return myself;
  }

  @Override
  public TinkerVertexAssert hasProperty(String propertyName, Object value) {
    hasProperty(propertyName);
    assertThat(actual.properties(propertyName))
        .toIterable()
        .extracting(Property::value)
        .contains(value);
    return myself;
  }
}
