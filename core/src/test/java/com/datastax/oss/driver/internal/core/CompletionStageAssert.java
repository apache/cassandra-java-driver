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
package com.datastax.oss.driver.internal.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.assertj.core.api.AbstractAssert;

public class CompletionStageAssert<V>
    extends AbstractAssert<CompletionStageAssert<V>, CompletionStage<V>> {

  public CompletionStageAssert(CompletionStage<V> actual) {
    super(actual, CompletionStageAssert.class);
  }

  public CompletionStageAssert<V> isSuccess(Consumer<V> valueAssertions) {
    try {
      V value = actual.toCompletableFuture().get(2, TimeUnit.SECONDS);
      valueAssertions.accept(value);
    } catch (TimeoutException e) {
      fail("Future did not complete within the timeout");
    } catch (Throwable t) {
      fail("Unexpected error while waiting on the future", t);
    }
    return this;
  }

  public CompletionStageAssert<V> isSuccess() {
    return isSuccess(v -> {});
  }

  public CompletionStageAssert<V> isFailed(Consumer<Throwable> failureAssertions) {
    try {
      actual.toCompletableFuture().get(2, TimeUnit.SECONDS);
      fail("Expected completion stage to fail");
    } catch (TimeoutException e) {
      fail("Future did not complete within the timeout");
    } catch (InterruptedException e) {
      fail("Interrupted while waiting for future to fail");
    } catch (ExecutionException e) {
      failureAssertions.accept(e.getCause());
    }
    return this;
  }

  public CompletionStageAssert<V> isFailed() {
    return isFailed(f -> {});
  }

  public CompletionStageAssert<V> isCancelled() {
    boolean cancelled = false;
    try {
      actual.toCompletableFuture().get(2, TimeUnit.SECONDS);
    } catch (CancellationException e) {
      cancelled = true;
    } catch (Exception ignored) {
    }
    if (!cancelled) {
      fail("Expected completion stage to be cancelled");
    }
    return this;
  }

  public CompletionStageAssert<V> isNotCancelled() {
    boolean cancelled = false;
    try {
      actual.toCompletableFuture().get(2, TimeUnit.SECONDS);
    } catch (CancellationException e) {
      cancelled = true;
    } catch (Exception ignored) {
    }
    if (cancelled) {
      fail("Expected completion stage not to be cancelled");
    }
    return this;
  }

  public CompletionStageAssert<V> isDone() {
    assertThat(actual.toCompletableFuture().isDone())
        .overridingErrorMessage("Expected completion stage to be done")
        .isTrue();
    return this;
  }

  public CompletionStageAssert<V> isNotDone() {
    assertThat(actual.toCompletableFuture().isDone())
        .overridingErrorMessage("Expected completion stage not to be done")
        .isFalse();
    return this;
  }
}
