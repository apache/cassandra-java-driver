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
package com.datastax.oss.driver.internal.core.session.throttling;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

class MockThrottled implements Throttled {
  final CompletionStage<Void> started = new CompletableFuture<>();
  final CompletionStage<Boolean> ended = new CompletableFuture<>();
  final CountDownLatch canRelease;

  public MockThrottled() {
    this(new CountDownLatch(0));
  }

  /*
   * The releaseLatch can be provided to add some delay before the
   * task readiness/fail callbacks complete. This can be used, eg, to
   * imitate a slow callback.
   */
  public MockThrottled(CountDownLatch releaseLatch) {
    this.canRelease = releaseLatch;
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    started.toCompletableFuture().complete(null);
    awaitRelease();
    ended.toCompletableFuture().complete(wasDelayed);
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    started.toCompletableFuture().complete(null);
    awaitRelease();
    ended.toCompletableFuture().completeExceptionally(error);
  }

  private void awaitRelease() {
    Uninterruptibles.awaitUninterruptibly(canRelease);
  }
}
