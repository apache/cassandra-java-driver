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
package com.datastax.dse.driver.api.core.cql.continuous.reactive;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.internal.core.cql.continuous.reactive.ContinuousCqlRequestReactiveProcessor;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import org.reactivestreams.Publisher;

/**
 * A {@link Session} that offers utility methods to issue queries using reactive-style programming
 * and continuous paging, combined together.
 *
 * <p>Methods in this interface all return {@link ContinuousReactiveResultSet} instances. All
 * publishers support multiple subscriptions in a unicast fashion: each subscriber triggers an
 * independent request execution and gets its own copy of the results.
 *
 * <p>Also, note that the publishers may emit items to their subscribers on an internal driver IO
 * thread. Subscriber implementors are encouraged to abide by <a
 * href="https://github.com/reactive-streams/reactive-streams-jvm#2.2">Reactive Streams
 * Specification rule 2.2</a> and avoid performing heavy computations or blocking calls inside
 * {@link org.reactivestreams.Subscriber#onNext(Object) onNext} calls, as doing so could slow down
 * the driver and impact performance. Instead, they should asynchronously dispatch received signals
 * to their processing logic.
 *
 * @see ReactiveRow
 */
public interface ContinuousReactiveSession extends Session {

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query continuously and
   * emits all the results.
   *
   * <p>See {@link ContinuousSession} for more explanations about continuous paging.
   *
   * <p>This feature is only available with DataStax Enterprise. Executing continuous queries
   * against an Apache Cassandra&reg; cluster will result in a runtime error.
   *
   * @param query the query to execute.
   * @return The {@link Publisher} that will publish the returned results.
   */
  @NonNull
  default ContinuousReactiveResultSet executeContinuouslyReactive(@NonNull String query) {
    return executeContinuouslyReactive(SimpleStatement.newInstance(query));
  }

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query continuously and
   * emits all the results.
   *
   * <p>See {@link ContinuousSession} for more explanations about continuous paging.
   *
   * <p>This feature is only available with DataStax Enterprise. Executing continuous queries
   * against an Apache Cassandra&reg; cluster will result in a runtime error.
   *
   * @param statement the statement to execute.
   * @return The {@link Publisher} that will publish the returned results.
   */
  @NonNull
  default ContinuousReactiveResultSet executeContinuouslyReactive(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, ContinuousCqlRequestReactiveProcessor.CONTINUOUS_REACTIVE_RESULT_SET));
  }
}
