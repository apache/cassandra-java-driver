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
package com.datastax.dse.driver.api.core.graph.reactive;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.reactivestreams.Publisher;

/**
 * A {@link Publisher} of {@link ReactiveGraphNode}s returned by a {@link ReactiveGraphSession}.
 *
 * <p>By default, all implementations returned by the driver are cold, unicast, single-subscriber
 * only publishers. In other words, <em>they do not support multiple subscriptions</em>; consider
 * caching the results produced by such publishers if you need to consume them by more than one
 * downstream subscriber.
 *
 * <p>Also, note that reactive graph result sets may emit items to their subscribers on an internal
 * driver IO thread. Subscriber implementors are encouraged to abide by <a
 * href="https://github.com/reactive-streams/reactive-streams-jvm#2.2">Reactive Streams
 * Specification rule 2.2</a> and avoid performing heavy computations or blocking calls inside
 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} calls, as doing so could slow down
 * the driver and impact performance. Instead, they should asynchronously dispatch received signals
 * to their processing logic.
 *
 * <p>This interface exists mainly to expose useful information about {@linkplain
 * #getExecutionInfos() request execution}. This is particularly convenient for queries that do not
 * return rows; for queries that do return rows, it is also possible, and oftentimes easier, to
 * access that same information {@linkplain ReactiveGraphNode at node level}.
 *
 * @see ReactiveGraphSession#executeReactive(GraphStatement)
 * @see ReactiveGraphNode
 */
public interface ReactiveGraphResultSet extends Publisher<ReactiveGraphNode> {

  /**
   * Returns {@linkplain ExecutionInfo information about the execution} of all requests that have
   * been performed so far to assemble this result set.
   *
   * <p>If the query is not paged, this publisher will emit exactly one item as soon as the response
   * arrives, then complete. If the query is paged, it will emit multiple items, one per page; then
   * it will complete when the last page arrives. If the query execution fails, then this publisher
   * will fail with the same error.
   *
   * <p>By default, publishers returned by this method do not support multiple subscriptions.
   *
   * @see ReactiveGraphNode#getExecutionInfo()
   */
  @NonNull
  Publisher<? extends ExecutionInfo> getExecutionInfos();
}
