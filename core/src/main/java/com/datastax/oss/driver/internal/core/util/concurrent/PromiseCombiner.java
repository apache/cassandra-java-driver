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

import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import net.jcip.annotations.ThreadSafe;

/**
 * A thread-safe version of Netty's {@link io.netty.util.concurrent.PromiseCombiner} that uses
 * proper synchronization to trigger the completion of the aggregate promise.
 */
@ThreadSafe
public class PromiseCombiner {

  /**
   * Combines the given futures into the given promise, that is, ties the completion of the latter
   * to that of the formers.
   *
   * @param aggregatePromise The promise that will complete when all parents complete.
   * @param parents The parent futures.
   */
  public static void combine(
      @NonNull Promise<Void> aggregatePromise, @NonNull Future<?>... parents) {
    PromiseCombinerListener listener =
        new PromiseCombinerListener(aggregatePromise, parents.length);
    for (Future<?> parent : parents) {
      parent.addListener(listener);
    }
  }

  private static class PromiseCombinerListener implements GenericFutureListener<Future<Object>> {

    private final Promise<Void> aggregatePromise;
    private final AtomicInteger remainingCount;
    private final AtomicReference<Throwable> aggregateFailureRef = new AtomicReference<>();

    private PromiseCombinerListener(Promise<Void> aggregatePromise, int numberOfParents) {
      this.aggregatePromise = aggregatePromise;
      remainingCount = new AtomicInteger(numberOfParents);
    }

    @Override
    public void operationComplete(Future<Object> future) {
      if (!future.isSuccess()) {
        aggregateFailureRef.updateAndGet(
            aggregateFailure -> {
              if (aggregateFailure == null) {
                aggregateFailure = future.cause();
              } else {
                aggregateFailure.addSuppressed(future.cause());
              }
              return aggregateFailure;
            });
      }
      if (remainingCount.decrementAndGet() == 0) {
        Throwable aggregateFailure = aggregateFailureRef.get();
        if (aggregateFailure != null) {
          aggregatePromise.tryFailure(aggregateFailure);
        } else {
          aggregatePromise.trySuccess(null);
        }
      }
    }
  }
}
