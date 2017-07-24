/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.session.Request;
import org.assertj.core.api.Assert;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import static org.assertj.core.api.Assertions.assertThat;

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
        policy.onReadTimeout(request, cl, blockFor, received, dataPresent, retryCount));
  }

  protected Assert<?, RetryDecision> assertOnWriteTimeout(
      ConsistencyLevel cl, WriteType writeType, int blockFor, int received, int retryCount) {
    return assertThat(
        policy.onWriteTimeout(request, cl, writeType, blockFor, received, retryCount));
  }

  protected Assert<?, RetryDecision> assertOnUnavailable(
      ConsistencyLevel cl, int required, int alive, int retryCount) {
    return assertThat(policy.onUnavailable(request, cl, required, alive, retryCount));
  }

  protected Assert<?, RetryDecision> assertOnRequestAborted(
      Class<? extends Throwable> errorClass, int retryCount) {
    return assertThat(policy.onRequestAborted(request, Mockito.mock(errorClass), retryCount));
  }

  protected Assert<?, RetryDecision> assertOnErrorResponse(
      Class<? extends CoordinatorException> errorClass, int retryCount) {
    return assertThat(policy.onErrorResponse(request, Mockito.mock(errorClass), retryCount));
  }
}
