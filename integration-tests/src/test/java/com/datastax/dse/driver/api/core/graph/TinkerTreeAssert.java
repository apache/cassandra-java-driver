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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.assertj.core.api.MapAssert;

public class TinkerTreeAssert<T> extends MapAssert<T, Tree<T>> {

  public TinkerTreeAssert(Tree<T> actual) {
    super(actual);
  }

  public TinkerTreeAssert<T> hasTree(T key) {
    assertThat(actual).containsKey(key);
    return this;
  }

  public TinkerTreeAssert<T> isLeaf() {
    assertThat(actual).hasSize(0);
    return this;
  }

  public TinkerTreeAssert<T> tree(T key) {
    hasTree(key);
    return new TinkerTreeAssert<>(actual.get(key));
  }
}
