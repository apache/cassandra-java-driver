/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import com.datastax.dse.driver.api.core.cql.continuous.reactive.ContinuousReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.internal.core.cql.continuous.reactive.ContinuousCqlRequestReactiveProcessor;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.reactivestreams.Publisher;

/**
 * A {@link ReactiveResultSet} that immediately signals the error passed at instantiation to all its
 * subscribers.
 *
 * @see CqlRequestReactiveProcessor#newFailure(java.lang.RuntimeException)
 * @see ContinuousCqlRequestReactiveProcessor#newFailure(java.lang.RuntimeException)
 */
public class FailedReactiveResultSet extends FailedPublisher<ReactiveRow>
    implements ReactiveResultSet, ContinuousReactiveResultSet {

  public FailedReactiveResultSet(Throwable error) {
    super(error);
  }

  @NonNull
  @Override
  public Publisher<? extends ColumnDefinitions> getColumnDefinitions() {
    return new FailedPublisher<>(error);
  }

  @NonNull
  @Override
  public Publisher<? extends ExecutionInfo> getExecutionInfos() {
    return new FailedPublisher<>(error);
  }

  @NonNull
  @Override
  public Publisher<Boolean> wasApplied() {
    return new FailedPublisher<>(error);
  }
}
