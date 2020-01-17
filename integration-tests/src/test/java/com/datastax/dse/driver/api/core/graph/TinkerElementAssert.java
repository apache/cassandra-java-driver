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
import org.assertj.core.api.AbstractAssert;

public abstract class TinkerElementAssert<S extends AbstractAssert<S, A>, A extends Element>
    extends AbstractAssert<S, A> {

  protected TinkerElementAssert(A actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public S hasId(Object id) {
    assertThat(actual.id()).isEqualTo(id);
    return myself;
  }

  public S hasLabel(String label) {
    assertThat(actual.label()).isEqualTo(label);
    return myself;
  }

  public S hasProperty(String propertyName) {
    assertThat(actual.property(propertyName).isPresent()).isTrue();
    return myself;
  }

  public S hasProperty(String propertyName, Object value) {
    hasProperty(propertyName);
    assertThat(actual.property(propertyName).value()).isEqualTo(value);
    return myself;
  }
}
