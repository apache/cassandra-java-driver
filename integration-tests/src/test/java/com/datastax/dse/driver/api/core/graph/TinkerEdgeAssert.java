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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.assertions.Assertions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerEdgeAssert extends TinkerElementAssert<TinkerEdgeAssert, Edge> {

  public TinkerEdgeAssert(Edge actual) {
    super(actual, TinkerEdgeAssert.class);
  }

  public TinkerEdgeAssert hasInVLabel(String label) {
    Assertions.assertThat(actual.inVertex().label()).isEqualTo(label);
    return myself;
  }

  public TinkerEdgeAssert hasOutVLabel(String label) {
    Assertions.assertThat(actual.outVertex().label()).isEqualTo(label);
    return myself;
  }

  public TinkerEdgeAssert hasOutV(Vertex vertex) {
    Assertions.assertThat(actual.outVertex()).isEqualTo(vertex);
    return myself;
  }

  public TinkerEdgeAssert hasInV(Vertex vertex) {
    Assertions.assertThat(actual.inVertex()).isEqualTo(vertex);
    return myself;
  }
}
