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
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.Queue;
import net.jcip.annotations.ThreadSafe;

/**
 * A specialized, thread-safe node queue for use when creating {@linkplain
 * com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy#newQueryPlan(Request, Session)
 * query plans}.
 *
 * <p>This interface and its built-in implementations are not general-purpose queues; they are
 * tailored for the specific use case of creating query plans in the driver. They make a few
 * unconventional API choices for the sake of performance.
 *
 * <p>Furthermore, the driver only consumes query plans through calls to its {@link #poll()} method;
 * therefore, this method is the only valid mutation operation for a query plan, other mutating
 * methods throw.
 *
 * <p>Both {@link #size()} and {@link #iterator()} are supported and never throw, even if called
 * concurrently. These methods are implemented for reporting purposes only, the driver itself does
 * not use them.
 *
 * <p>All built-in {@link QueryPlan} implementations can be safely reused for custom load balancing
 * policies; if you plan to do so, study the source code of {@link
 * com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy} or {@link
 * com.datastax.oss.driver.internal.core.loadbalancing.BasicLoadBalancingPolicy}.
 *
 * @see QueryPlanBase
 */
@ThreadSafe
public interface QueryPlan extends Queue<Node> {

  QueryPlan EMPTY = new EmptyQueryPlan();

  /**
   * {@inheritDoc}
   *
   * <p>Implementation note: query plan iterators are snapshots that reflect the contents of the
   * queue at the time of the call, and are not affected by further modifications. Successive calls
   * to this method will return different objects.
   */
  @NonNull
  @Override
  Iterator<Node> iterator();

  @Override
  default boolean offer(Node node) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  default Node peek() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  default boolean add(Node node) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  default Node remove() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  default Node element() {
    throw new UnsupportedOperationException("Not implemented");
  }
}
