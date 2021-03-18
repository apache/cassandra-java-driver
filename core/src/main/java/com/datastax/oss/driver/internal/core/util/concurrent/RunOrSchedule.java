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

import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

/**
 * Utility to run a task on a Netty event executor (i.e. thread). If we're already on the executor,
 * the task is submitted, otherwise it's scheduled.
 *
 * <p>Be careful when using this, always keep in mind that the task might be executed synchronously.
 * This can lead to subtle bugs when both the calling code and the callback manipulate a collection:
 *
 * <pre>{@code
 * List<CompletionStage<Foo>> futureFoos;
 *
 * // Scheduled on eventExecutor:
 * for (int i = 0; i < count; i++) {
 *   CompletionStage<Foo> futureFoo = FooFactory.init();
 *   futureFoos.add(futureFoo);
 *   // futureFoo happens to be complete by now, so callback gets executed immediately
 *   futureFoo.whenComplete(RunOrSchedule.on(eventExecutor, () -> callback(futureFoo)));
 * }
 *
 * private void callback(CompletionStage<Foo> futureFoo) {
 *    futureFoos.remove(futureFoo); // ConcurrentModificationException!!!
 * }
 * }</pre>
 *
 * For that kind of situation, it's better to use {@code futureFoo.whenCompleteAsync(theTask,
 * eventExecutor)}, so that the task is always scheduled.
 */
public class RunOrSchedule {

  public static void on(EventExecutor executor, Runnable task) {
    if (executor.inEventLoop()) {
      task.run();
    } else {
      executor.submit(task).addListener(UncaughtExceptions::log);
    }
  }

  public static <T> Consumer<T> on(EventExecutor executor, Consumer<T> task) {
    return (t) -> {
      if (executor.inEventLoop()) {
        task.accept(t);
      } else {
        executor.submit(() -> task.accept(t)).addListener(UncaughtExceptions::log);
      }
    };
  }

  public static <T> CompletionStage<T> on(
      EventExecutor executor, Callable<CompletionStage<T>> task) {
    if (executor.inEventLoop()) {
      try {
        return task.call();
      } catch (Exception e) {
        return CompletableFutures.failedFuture(e);
      }
    } else {
      CompletableFuture<T> result = new CompletableFuture<>();
      executor
          .submit(task)
          .addListener(
              (Future<CompletionStage<T>> f) -> {
                if (f.isSuccess()) {
                  CompletableFutures.completeFrom(f.getNow(), result);
                } else {
                  result.completeExceptionally(f.cause());
                }
              });
      return result;
    }
  }
}
