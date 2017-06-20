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
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.session.Request;

/**
 * Defines the behavior to adopt when a request fails.
 *
 * <p>For each request, the driver gets a "query plan" (a list of coordinators to try) from the
 * {@link LoadBalancingPolicy}, and tries each node in sequence. This policy is invoked if the
 * request to that node fails.
 */
public interface RetryPolicy extends AutoCloseable {

  RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount);

  RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel cl,
      WriteType writeType,
      int blockFor,
      int received,
      int retryCount);

  RetryDecision onUnavailable(
      Request request, ConsistencyLevel cl, int required, int alive, int retryCount);

  RetryDecision onRequestAborted(Request request, Throwable error, int retryCount);

  RetryDecision onErrorResponse(Request request, Throwable error, int retryCount);

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();
}
