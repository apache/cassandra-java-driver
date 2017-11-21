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
package com.datastax.oss.driver.internal.core.util.concurrent;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverExecutionException;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletableFutures {

  public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(cause);
    return future;
  }

  /** Completes {@code target} with the outcome of {@code source}. */
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
  public static <T> CompletionStage<Void> allDone(List<CompletionStage<T>> inputs) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (inputs.isEmpty()) {
      result.complete(null);
    } else {
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
    }
    return result;
  }

  /** Do something when all inputs are done (success or failure). */
  public static <T> void whenAllDone(
      List<CompletionStage<T>> inputs, Runnable callback, Executor executor) {
    allDone(inputs)
        .thenAcceptAsync(success -> callback.run(), executor)
        .exceptionally(UncaughtExceptions::log);
  }

  /** Get the result now, when we know for sure that the future is complete. */
  public static <T> T getCompleted(CompletableFuture<T> future) {
    Preconditions.checkArgument(future.isDone() && !future.isCompletedExceptionally());
    try {
      return future.get();
    } catch (InterruptedException | ExecutionException e) {
      // Neither can happen given the precondition
      throw new AssertionError("Unexpected error", e);
    }
  }

  /** Get the error now, when we know for sure that the future is failed. */
  public static Throwable getFailed(CompletableFuture<?> future) {
    Preconditions.checkArgument(future.isCompletedExceptionally());
    try {
      future.get();
      throw new AssertionError("future should be failed");
    } catch (InterruptedException e) {
      throw new AssertionError("Unexpected error", e);
    } catch (ExecutionException e) {
      return e.getCause();
    }
  }

  public static <T> T getUninterruptibly(CompletionStage<T> stage) {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          return stage.toCompletableFuture().get();
        } catch (InterruptedException e) {
          interrupted = true;
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          if (cause instanceof DriverException) {
            throw ((DriverException) cause).copy();
          }
          Throwables.throwIfUnchecked(cause);
          throw new DriverExecutionException(cause);
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
