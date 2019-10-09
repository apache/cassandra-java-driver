/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.cql.continuous.reactive;

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
   * @param statement the statement to execute.
   * @return The {@link Publisher} that will publish the returned results.
   */
  @NonNull
  default ContinuousReactiveResultSet executeContinuouslyReactive(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, ContinuousCqlRequestReactiveProcessor.CONTINUOUS_REACTIVE_RESULT_SET));
  }
}
