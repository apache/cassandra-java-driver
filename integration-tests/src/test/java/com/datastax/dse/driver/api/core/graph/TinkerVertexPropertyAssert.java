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
