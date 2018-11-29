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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.concurrent.CompletionStage;

/**
 * An object that can be closed in an asynchronous, non-blocking manner.
 *
 * <p>For convenience, this extends the JDK's {@code AutoCloseable} in order to be usable in
 * try-with-resource blocks (in that case, the <em>blocking</em> {@link #close()} will be used).
 */
public interface AsyncAutoCloseable extends AutoCloseable {

  /**
   * Returns a stage that will complete when {@link #close()} or {@link #forceCloseAsync()} is
   * called, and the shutdown sequence completes.
   */
  @NonNull
  CompletionStage<Void> closeFuture();

  /**
   * Whether shutdown has completed.
   *
   * <p>This is a shortcut for {@code closeFuture().toCompletableFuture().isDone()}.
   */
  default boolean isClosed() {
    return closeFuture().toCompletableFuture().isDone();
  }

  /**
   * Initiates an orderly shutdown: no new requests are accepted, but all pending requests are
   * allowed to complete normally.
   *
   * @return a stage that will complete when the shutdown sequence is complete. Multiple calls to
   *     this method or {@link #forceCloseAsync()} always return the same instance.
   */
  @NonNull
  CompletionStage<Void> closeAsync();

  /**
   * Initiates a forced shutdown of this instance: no new requests are accepted, and all pending
   * requests will complete with an exception.
   *
   * @return a stage that will complete when the shutdown sequence is complete. Multiple calls to
   *     this method or {@link #close()} always return the same instance.
   */
  @NonNull
  CompletionStage<Void> forceCloseAsync();

  /**
   * {@inheritDoc}
   *
   * <p>This method is implemented by calling {@link #closeAsync()} and blocking on the result. This
   * should not be called on a driver thread.
   */
  @Override
  default void close() {
    BlockingOperation.checkNotDriverThread();
    CompletableFutures.getUninterruptibly(closeAsync().toCompletableFuture());
  }
}
