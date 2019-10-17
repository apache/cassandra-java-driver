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
package com.datastax.dse.driver.internal.mapper.reactive;

import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.dse.driver.internal.core.cql.reactive.FailedPublisher;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.reactivestreams.Publisher;

/**
 * A mapped reactive result set that immediately signals the error passed at instantiation to all
 * its subscribers.
 */
public class FailedMappedReactiveResultSet<EntityT> extends FailedPublisher<EntityT>
    implements MappedReactiveResultSet<EntityT> {

  public FailedMappedReactiveResultSet(Throwable error) {
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
