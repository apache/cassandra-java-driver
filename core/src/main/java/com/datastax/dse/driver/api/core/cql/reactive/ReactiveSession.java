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
package com.datastax.dse.driver.api.core.cql.reactive;

import com.datastax.dse.driver.internal.core.cql.reactive.CqlRequestReactiveProcessor;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import org.reactivestreams.Publisher;

/**
 * A {@link Session} that offers utility methods to issue queries using reactive-style programming.
 *
 * <p>Methods in this interface all return {@link ReactiveResultSet} instances. See the javadocs of
 * this interface for important remarks anc caveats regarding the subscription to and consumption of
 * reactive result sets.
 *
 * @see ReactiveResultSet
 * @see ReactiveRow
 */
public interface ReactiveSession extends Session {

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query and emits all
   * the results.
   *
   * <p>This is an alias for {@link #executeReactive(Statement)}
   * executeReactive(SimpleStatement.newInstance(query))}.
   *
   * @param query the query to execute.
   * @return The {@link Publisher} that will publish the returned results.
   * @see SimpleStatement#newInstance(String)
   */
  @NonNull
  default ReactiveResultSet executeReactive(@NonNull String query) {
    return executeReactive(SimpleStatement.newInstance(query));
  }

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query and emits all
   * the results.
   *
   * <p>This is an alias for {@link #executeReactive(Statement)}
   * executeReactive(SimpleStatement.newInstance(query, values))}.
   *
   * @param query the query to execute.
   * @param values the values for placeholders in the query string. Individual values can be {@code
   *     null}, but the vararg array itself can't.
   * @return The {@link Publisher} that will publish the returned results.
   * @see SimpleStatement#newInstance(String,Object...)
   */
  @NonNull
  default ReactiveResultSet executeReactive(@NonNull String query, @NonNull Object... values) {
    return executeReactive(SimpleStatement.newInstance(query, values));
  }

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query and emits all
   * the results.
   *
   * <p>This is an alias for {@link #executeReactive(Statement)}
   * executeReactive(SimpleStatement.newInstance(query,values))}.
   *
   * @param query the query to execute.
   * @param values the values for named placeholders in the query string. Individual values can be
   *     {@code null}, but the map itself can't.
   * @return The {@link Publisher} that will publish the returned results.
   * @see SimpleStatement#newInstance(String,Map)
   */
  @NonNull
  default ReactiveResultSet executeReactive(
      @NonNull String query, @NonNull Map<String, Object> values) {
    return executeReactive(SimpleStatement.newInstance(query, values));
  }

  /**
   * Returns a {@link Publisher} that, once subscribed to, executes the given query and emits all
   * the results.
   *
   * @param statement the statement to execute.
   * @return The {@link Publisher} that will publish the returned results.
   */
  @NonNull
  default ReactiveResultSet executeReactive(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, CqlRequestReactiveProcessor.REACTIVE_RESULT_SET));
  }
}
