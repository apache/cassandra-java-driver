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
