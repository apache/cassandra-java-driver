package com.datastax.driver.core;

import com.google.common.util.concurrent.AbstractFuture;

/**
 * A simple future that can be set to a value.
 *
 * Note: this is equivalent to guava SettableFuture, but the latter is a final class.
 */
class SimpleFuture<V> extends AbstractFuture<V> {

  /**
   * Creates a new {@code SimpleFuture}.
   */
  public static <V> SimpleFuture<V> create() {
    return new SimpleFuture<V>();
  }

  protected SimpleFuture() {}

  /**
   * Sets the value of this future.  This method will return {@code true} if
   * the value was successfully set, or {@code false} if the future has already
   * been set or cancelled.
   *
   * @param value the value the future should hold.
   * @return true if the value was successfully set.
   */
  @Override
  public boolean set(V value) {
    return super.set(value);
  }

  /**
   * Sets the future to having failed with the given exception. This exception
   * will be wrapped in an {@code ExecutionException} and thrown from the {@code
   * get} methods. This method will return {@code true} if the exception was
   * successfully set, or {@code false} if the future has already been set or
   * cancelled.
   *
   * @param throwable the exception the future should hold.
   * @return true if the exception was successfully set.
   */
  @Override
  public boolean setException(Throwable throwable) {
    return super.setException(throwable);
  }
}
