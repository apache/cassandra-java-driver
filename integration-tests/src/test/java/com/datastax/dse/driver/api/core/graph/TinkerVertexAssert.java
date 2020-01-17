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
