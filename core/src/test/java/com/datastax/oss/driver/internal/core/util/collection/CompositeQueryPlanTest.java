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
package com.datastax.oss.driver.internal.core.util.collection;

import com.datastax.oss.driver.api.core.metadata.Node;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CompositeQueryPlanTest extends QueryPlanTestBase {

  @Override
  protected QueryPlan newQueryPlan(Node... nodes) {
    Object[] n1 = new Object[nodes.length / 2];
    Object[] n2 = new Object[nodes.length - n1.length];
    System.arraycopy(nodes, 0, n1, 0, n1.length);
    System.arraycopy(nodes, n1.length, n2, 0, n2.length);
    return new CompositeQueryPlan(
        new SimpleQueryPlan(n1),
        new LazyQueryPlan() {
          @Override
          protected Object[] computeNodes() {
            return n2;
          }
        });
  }
}
