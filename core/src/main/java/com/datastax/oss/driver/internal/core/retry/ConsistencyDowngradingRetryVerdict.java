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
package com.datastax.oss.driver.internal.core.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

public class ConsistencyDowngradingRetryVerdict implements RetryVerdict {

  private final ConsistencyLevel consistencyLevel;

  public ConsistencyDowngradingRetryVerdict(@NonNull ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  @NonNull
  @Override
  public RetryDecision getRetryDecision() {
    return RetryDecision.RETRY_SAME;
  }

  @NonNull
  @Override
  public <RequestT extends Request> RequestT getRetryRequest(@NonNull RequestT previous) {
    if (previous instanceof Statement) {
      Statement<?> statement = (Statement<?>) previous;
      @SuppressWarnings("unchecked")
      RequestT toRetry = (RequestT) statement.setConsistencyLevel(consistencyLevel);
      return toRetry;
    }
    return previous;
  }

  @Override
  public String toString() {
    return getRetryDecision() + " at consistency " + consistencyLevel;
  }
}
