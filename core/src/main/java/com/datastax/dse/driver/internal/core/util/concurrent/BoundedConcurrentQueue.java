/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.util.concurrent;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A concurrent queue with a limited size.
 *
 * <p>Once the queue is full, the insertion of the next element is delayed until space becomes
 * available again; in the meantime, additional insertions are not allowed (in other words, there
 * can be at most one "pending" element waiting on a full queue).
 */
public class BoundedConcurrentQueue<ElementT> {

  private final Deque<ElementT> elements = new ConcurrentLinkedDeque<>();
  private final AtomicReference<State> state;

  public BoundedConcurrentQueue(int maxSize) {
    this.state = new AtomicReference<>(new State(maxSize));
  }

  /**
   * @return a stage that completes when the element is inserted. If there was still space in the
   *     queue, it will be already complete; if the queue was full, it will complete at a later
   *     point in time (triggered by a call to {@link #poll()}). <b>This method must not be invoked
   *     again until the stage has completed</b>.
   * @throws IllegalStateException if the method is invoked before the stage returned by the
   *     previous call has completed.
   */
  @NonNull
  public CompletionStage<ElementT> offer(@NonNull ElementT element) {
    while (true) {
      State oldState = state.get();
      State newState = oldState.increment();
      if (state.compareAndSet(oldState, newState)) {
        if (newState.spaceAvailable != null) {
          return newState.spaceAvailable.thenApply(
              (aVoid) -> {
                elements.offer(element);
                return element;
              });
        } else {
          elements.offer(element);
          return CompletableFuture.completedFuture(element);
        }
      }
    }
  }

  @Nullable
  public ElementT poll() {
    while (true) {
      State oldState = state.get();
      if (oldState.size == 0) {
        return null;
      }
      State newState = oldState.decrement();
      if (state.compareAndSet(oldState, newState)) {
        if (oldState.spaceAvailable != null) {
          oldState.spaceAvailable.complete(null);
        }
        return elements.poll();
      }
    }
  }

  @Nullable
  public ElementT peek() {
    return elements.peek();
  }

  /**
   * Note that this does not complete a pending call to {@link #offer(Object)}. We only use this
   * method for terminal states where we want to dereference the contained elements.
   */
  public void clear() {
    elements.clear();
  }

  private static class State {

    private final int maxSize;

    final int size; // Number of elements in the queue, + 1 if one is waiting to get in
    final CompletableFuture<Void> spaceAvailable; // Not null iff size == maxSize + 1

    State(int maxSize) {
      this(0, null, maxSize);
    }

    private State(int size, CompletableFuture<Void> spaceAvailable, int maxSize) {
      this.maxSize = maxSize;
      this.size = size;
      this.spaceAvailable = spaceAvailable;
    }

    State increment() {
      if (size > maxSize) {
        throw new IllegalStateException(
            "Can't call offer() until the stage returned by the previous offer() call has completed");
      }
      int newSize = size + 1;
      CompletableFuture<Void> newFuture =
          (newSize == maxSize + 1) ? new CompletableFuture<>() : null;
      return new State(newSize, newFuture, maxSize);
    }

    State decrement() {
      return new State(size - 1, null, maxSize);
    }
  }
}
