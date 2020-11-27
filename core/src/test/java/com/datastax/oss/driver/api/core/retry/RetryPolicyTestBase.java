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
package com.datastax.oss.driver.api.core.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import org.assertj.core.api.Assert;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class RetryPolicyTestBase {
  private final RetryPolicy policy;

  @Mock private Request request;

  protected RetryPolicyTestBase(RetryPolicy policy) {
    this.policy = policy;
  }

  protected Assert<?, RetryDecision> assertOnReadTimeout(
      ConsistencyLevel cl, int blockFor, int received, boolean dataPresent, int retryCount) {
    return assertThat(
        policy
            .onReadTimeoutVerdict(request, cl, blockFor, received, dataPresent, retryCount)
            .getRetryDecision());
  }

  protected Assert<?, RetryDecision> assertOnWriteTimeout(
      ConsistencyLevel cl, WriteType writeType, int blockFor, int received, int retryCount) {
    return assertThat(
        policy
            .onWriteTimeoutVerdict(request, cl, writeType, blockFor, received, retryCount)
            .getRetryDecision());
  }

  protected Assert<?, RetryDecision> assertOnUnavailable(
      ConsistencyLevel cl, int required, int alive, int retryCount) {
    return assertThat(
        policy.onUnavailableVerdict(request, cl, required, alive, retryCount).getRetryDecision());
  }

  protected Assert<?, RetryDecision> assertOnRequestAborted(
      Class<? extends Throwable> errorClass, int retryCount) {
    return assertThat(
        policy.onRequestAbortedVerdict(request, mock(errorClass), retryCount).getRetryDecision());
  }

  protected Assert<?, RetryDecision> assertOnErrorResponse(
      Class<? extends CoordinatorException> errorClass, int retryCount) {
    return assertThat(
        policy.onErrorResponseVerdict(request, mock(errorClass), retryCount).getRetryDecision());
  }
}
