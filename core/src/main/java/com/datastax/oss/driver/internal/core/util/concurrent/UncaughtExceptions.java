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

import com.datastax.oss.driver.internal.core.util.Loggers;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to log unexpected exceptions in asynchronous tasks.
 *
 * <p>Use this whenever you execute a future callback to apply side effects, but throw away the
 * future itself:
 *
 * <pre>{@code
 * CompletionStage<Foo> futureFoo = FooFactory.build();
 *
 * futureFoo
 *   .whenComplete((f, error) -> { handler code with side effects })
 *   // futureFoo is not propagated, do this or any unexpected error in the handler will be
 *   // swallowed
 *   .exceptionally(UncaughtExceptions::log);
 *
 * // If you return the future, you don't need it (but it's up to the caller to handle a failed
 * // future)
 * return futureFoo.whenComplete(...)
 * }</pre>
 */
public class UncaughtExceptions {

  private static final Logger LOG = LoggerFactory.getLogger(UncaughtExceptions.class);

  public static <T> void log(Future<T> future) {
    if (!future.isSuccess() && !future.isCancelled()) {
      Loggers.warnWithException(LOG, "Uncaught exception in scheduled task", future.cause());
    }
  }

  @SuppressWarnings("TypeParameterUnusedInFormals") // type parameter is only needed for chaining
  public static <T> T log(Throwable t) {
    Loggers.warnWithException(LOG, "Uncaught exception in scheduled task", t);
    return null;
  }
}
