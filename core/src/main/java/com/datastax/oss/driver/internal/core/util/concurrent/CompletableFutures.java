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
package com.datastax.oss.driver.internal.core.util.concurrent;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletableFutures {

  public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(cause);
    return future;
  }

  /** Completes {@code target} with the outcome of {@code source} */
  public static <T> void completeFrom(CompletionStage<T> source, CompletableFuture<T> target) {
    source.whenComplete(
        (t, error) -> {
          if (error != null) {
            target.completeExceptionally(error);
          } else {
            target.complete(t);
          }
        });
  }

  /** @return a completion stage that completes when all inputs are done (success or failure). */
  public static <T> CompletionStage<Void> whenAllDone(List<CompletionStage<T>> inputs) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    final int todo = inputs.size();
    final AtomicInteger done = new AtomicInteger();
    for (CompletionStage<?> input : inputs) {
      input.whenComplete(
          (v, error) -> {
            if (done.incrementAndGet() == todo) {
              result.complete(null);
            }
          });
    }
    return result;
  }
}
