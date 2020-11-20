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
import io.netty.util.concurrent.FastThreadLocalThread;
import java.util.concurrent.ThreadFactory;

/**
 * Safeguards against bad usage patterns in client code that could introduce deadlocks in the
 * driver.
 *
 * <p>The driver internals are fully asynchronous, nothing should ever block. On the other hand, our
 * API exposes synchronous wrappers, that call async methods and wait on the result (as a
 * convenience for clients that don't want to do async). These methods should never be called on a
 * driver thread, because this can lead to deadlocks. This can happen from client code if it uses
 * callbacks.
 */
public class BlockingOperation {

  /**
   * This method is invoked from each synchronous driver method, and checks that we are not on a
   * driver thread.
   *
   * <p>For this to work, all driver threads must be created by {@link SafeThreadFactory} (which is
   * the case by default).
   *
   * @throws IllegalStateException if a driver thread is executing this.
   */
  public static void checkNotDriverThread() {
    if (Thread.currentThread() instanceof InternalThread) {
      throw new IllegalStateException(
          "Detected a synchronous API call on a driver thread, "
              + "failing because this can cause deadlocks.");
    }
  }

  /**
   * Marks threads as driver threads, so that they will be detected by {@link
   * #checkNotDriverThread()}
   */
  public static class SafeThreadFactory implements ThreadFactory {
    @Override
    public Thread newThread(@NonNull Runnable r) {
      return new InternalThread(r);
    }
  }

  static class InternalThread extends FastThreadLocalThread {
    private InternalThread(Runnable runnable) {
      super(runnable);
    }
  }
}
