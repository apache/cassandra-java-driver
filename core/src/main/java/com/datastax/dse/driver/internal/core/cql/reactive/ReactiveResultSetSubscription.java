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
package com.datastax.dse.driver.internal.core.cql.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.internal.core.util.concurrent.BoundedConcurrentQueue;
import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import net.jcip.annotations.ThreadSafe;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A single-subscriber subscription that executes one single query and emits all the returned rows.
 *
 * <p>This class can handle both continuous and non-continuous result sets.
 */
@ThreadSafe
public class ReactiveResultSetSubscription<ResultSetT extends AsyncPagingIterable<Row, ResultSetT>>
    implements Subscription {

  private static final Logger LOG = LoggerFactory.getLogger(ReactiveResultSetSubscription.class);

  private static final int MAX_ENQUEUED_PAGES = 4;

  /** Tracks the number of items requested by the subscriber. */
  private final AtomicLong requested = new AtomicLong(0);

  /** The pages received so far, with a maximum of MAX_ENQUEUED_PAGES elements. */
  private final BoundedConcurrentQueue<Page<ResultSetT>> pages =
      new BoundedConcurrentQueue<>(MAX_ENQUEUED_PAGES);

  /**
   * Used to signal that a thread is currently draining, i.e., emitting items to the subscriber.
   * When it is zero, that means there is no ongoing emission. This mechanism effectively serializes
   * access to the drain() method, and also keeps track of missed attempts to enter it, since each
   * thread that attempts to drain will increment this counter.
   *
   * @see #drain()
   */
  private final AtomicInteger draining = new AtomicInteger(0);

  /**
   * Waited upon by the driver and completed when the subscriber requests its first item.
   *
   * <p>Used to hold off emitting results until the subscriber issues its first request for items.
   * Since this future is only completed from {@link #request(long)}, this effectively conditions
   * the enqueueing of the first page to the reception of the subscriber's first request.
   *
   * <p>This mechanism avoids sending terminal signals before a request is made when the stream is
   * empty. Note that as per 2.9, "a Subscriber MUST be prepared to receive an onComplete signal
   * with or without a preceding Subscription.request(long n) call." However, the TCK considers it
   * as unfair behavior.
   *
   * @see #start(Callable)
   * @see #request(long)
   */
  private final CompletableFuture<Void> firstSubscriberRequestArrived = new CompletableFuture<>();

  /** non-final because it has to be de-referenced, see {@link #clear()}. */
  private volatile Subscriber<? super ReactiveRow> mainSubscriber;

  private volatile Subscriber<ColumnDefinitions> columnDefinitionsSubscriber;

  private volatile Subscriber<ExecutionInfo> executionInfosSubscriber;

  private volatile Subscriber<Boolean> wasAppliedSubscriber;

  /**
   * Set to true when the subscription is cancelled, which happens when an error is encountered,
   * when the result set is fully consumed and the subscription terminates, or when the subscriber
   * manually calls {@link #cancel()}.
   */
  private volatile boolean cancelled = false;

  ReactiveResultSetSubscription(
      @NonNull Subscriber<? super ReactiveRow> mainSubscriber,
      @NonNull Subscriber<ColumnDefinitions> columnDefinitionsSubscriber,
      @NonNull Subscriber<ExecutionInfo> executionInfosSubscriber,
      @NonNull Subscriber<Boolean> wasAppliedSubscriber) {
    this.mainSubscriber = mainSubscriber;
    this.columnDefinitionsSubscriber = columnDefinitionsSubscriber;
    this.executionInfosSubscriber = executionInfosSubscriber;
    this.wasAppliedSubscriber = wasAppliedSubscriber;
  }

  /**
   * Starts the query execution.
   *
   * <p>Must be called immediately after creating the subscription, but after {@link
   * Subscriber#onSubscribe(Subscription)}.
   *
   * @param firstPage The future that, when complete, will produce the first page.
   */
  void start(@NonNull Callable<CompletionStage<ResultSetT>> firstPage) {
    firstSubscriberRequestArrived.thenAccept(
        (aVoid) -> fetchNextPageAndEnqueue(new Page<>(firstPage), true));
  }

  @Override
  public void request(long n) {
    // As per 3.6: after the Subscription is cancelled, additional
    // calls to request() MUST be NOPs.
    if (!cancelled) {
      if (n < 1) {
        // Validate request as per rule 3.9
        doOnError(
            new IllegalArgumentException(
                mainSubscriber
                    + " violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements."));
      } else {
        // As per rule 3.17, when demand overflows Long.MAX_VALUE
        // it can be treated as "effectively unbounded"
        ReactiveOperators.addCap(requested, n);
        // Set the first future to true if not done yet.
        // This will make the first page of results ready for consumption,
        // see start().
        // As per 2.7 it is the subscriber's responsibility to provide
        // external synchronization when calling request(),
        // so the check-then-act idiom below is good enough
        // (and besides, complete() is idempotent).
        if (!firstSubscriberRequestArrived.isDone()) {
          firstSubscriberRequestArrived.complete(null);
        }
        drain();
      }
    }
  }

  @Override
  public void cancel() {
    // As per 3.5: Subscription.cancel() MUST respect the responsiveness of
    // its caller by returning in a timely manner, MUST be idempotent and
    // MUST be thread-safe.
    if (!cancelled) {
      cancelled = true;
      if (draining.getAndIncrement() == 0) {
        // If nobody is draining, clear now;
        // otherwise, the draining thread will notice
        // that the cancelled flag was set
        // and will clear for us.
        clear();
      }
    }
  }

  /**
   * Attempts to drain available items, i.e. emit them to the subscriber.
   *
   * <p>Access to this method is serialized by the field {@link #draining}: only one thread at a
   * time can drain, but threads that attempt to drain while other thread is already draining
   * increment that field; the draining thread, before finishing its work, checks for such failed
   * attempts and triggers another round of draining if that was the case.
   *
   * <p>The loop is interrupted when 1) the requested amount has been met or 2) when there are no
   * more items readily available or 3) the subscription has been cancelled.
   *
   * <p>The loop also checks for stream exhaustion and emits a terminal {@code onComplete} signal in
   * this case.
   *
   * <p>This method may run on a driver IO thread when invoked from {@link
   * #fetchNextPageAndEnqueue(Page, boolean)}, or on a subscriber thread, when invoked from {@link
   * #request(long)}.
   */
  @SuppressWarnings("ConditionalBreakInInfiniteLoop")
  private void drain() {
    // As per 3.4: this method SHOULD respect the responsiveness
    // of its caller by returning in a timely manner.
    // We accomplish this by a wait-free implementation.
    if (draining.getAndIncrement() != 0) {
      // Someone else is already draining, so do nothing,
      // the other thread will notice that we attempted to drain.
      // This also allows to abide by rule 3.3 and avoid
      // cycles such as request() -> onNext() -> request() etc.
      return;
    }
    int missed = 1;
    // Note: when termination is detected inside this loop,
    // we MUST call clear() manually.
    for (; ; ) {
      // The requested number of items at this point
      long r = requested.get();
      // The number of items emitted thus far
      long emitted = 0L;
      while (emitted != r) {
        if (cancelled) {
          clear();
          return;
        }
        Object result;
        try {
          result = tryNext();
        } catch (Throwable t) {
          doOnError(t);
          clear();
          return;
        }
        if (result == null) {
          break;
        }
        if (result instanceof Throwable) {
          doOnError((Throwable) result);
          clear();
          return;
        }
        doOnNext((ReactiveRow) result);
        emitted++;
      }
      if (isExhausted()) {
        doOnComplete();
        clear();
        return;
      }
      if (cancelled) {
        clear();
        return;
      }
      if (emitted != 0) {
        // if any item was emitted, adjust the requested field
        ReactiveOperators.subCap(requested, emitted);
      }
      // if another thread tried to call drain() while we were busy,
      // then we should do another drain round.
      missed = draining.addAndGet(-missed);
      if (missed == 0) {
        break;
      }
    }
  }

  /**
   * Tries to return the next item, if one is readily available, and returns {@code null} otherwise.
   *
   * <p>Cannot run concurrently due to the {@link #draining} field.
   */
  @Nullable
  private Object tryNext() {
    Page current = pages.peek();
    if (current != null) {
      if (current.hasMoreRows()) {
        return current.nextRow();
      } else if (current.hasMorePages()) {
        // Discard current page as it is consumed.
        // Don't discard the last page though as we need it
        // to test isExhausted(). It will be GC'ed when a terminal signal
        // is issued anyway, so that's no big deal.
        if (pages.poll() == null) {
          throw new AssertionError("Queue is empty, this should not happen");
        }
        // if the next page is readily available,
        // serve its first row now, no need to wait
        // for the next drain.
        return tryNext();
      }
    }
    // No items available right now.
    return null;
  }

  /**
   * Returns {@code true} when the entire stream has been consumed and no more items can be emitted.
   * When that is the case, a terminal signal is sent.
   *
   * <p>Cannot run concurrently due to the draining field.
   */
  private boolean isExhausted() {
    Page current = pages.peek();
    // Note: current can only be null when:
    // 1) we are waiting for the first page and it hasn't arrived yet;
    // 2) we just discarded the current page, but the next page hasn't arrived yet.
    // In any case, a null here means it is not the last page, since the last page
    // stays in the queue until the very end of the operation.
    return current != null && !current.hasMoreRows() && !current.hasMorePages();
  }

  /**
   * Runs on a subscriber thread initially, see {@link #start(Callable)}. Subsequent executions run
   * on the thread that completes the pair of futures [current.fetchNextPage, pages.offer] and
   * enqueues. This can be a driver IO thread or a subscriber thread; in both cases, cannot run
   * concurrently due to the fact that one can only fetch the next page when the current one is
   * arrived and enqueued.
   */
  private void fetchNextPageAndEnqueue(@NonNull Page<ResultSetT> current, boolean firstPage) {
    current
        .fetchNextPage()
        // as soon as the response arrives,
        // create the new page
        .handle(
            (rs, t) -> {
              Page<ResultSetT> page;
              if (t == null) {
                page = toPage(rs);
                executionInfosSubscriber.onNext(rs.getExecutionInfo());
                if (!page.hasMorePages()) {
                  executionInfosSubscriber.onComplete();
                }
                if (firstPage) {
                  columnDefinitionsSubscriber.onNext(rs.getColumnDefinitions());
                  columnDefinitionsSubscriber.onComplete();
                  // Avoid calling wasApplied on empty pages as some implementations may throw
                  // IllegalStateException; if the page is empty, this wasn't a CAS query, in which
                  // case, as per the method's contract, wasApplied should be true.
                  boolean wasApplied = rs.remaining() == 0 || rs.wasApplied();
                  wasAppliedSubscriber.onNext(wasApplied);
                  wasAppliedSubscriber.onComplete();
                }
              } else {
                // Unwrap CompletionExceptions created by combined futures
                if (t instanceof CompletionException) {
                  t = t.getCause();
                }
                page = toErrorPage(t);
                executionInfosSubscriber.onError(t);
                if (firstPage) {
                  columnDefinitionsSubscriber.onError(t);
                  wasAppliedSubscriber.onError(t);
                }
              }
              return page;
            })
        .thenCompose(pages::offer)
        .thenAccept(
            page -> {
              if (page.hasMorePages() && !cancelled) {
                // preemptively fetch the next page, if available
                fetchNextPageAndEnqueue(page, false);
              }
              drain();
            });
  }

  private void doOnNext(@NonNull ReactiveRow result) {
    try {
      mainSubscriber.onNext(result);
    } catch (Throwable t) {
      LOG.error(
          mainSubscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onNext.",
          t);
      cancel();
    }
  }

  private void doOnComplete() {
    try {
      // Then we signal onComplete as per rules 1.2 and 1.5
      mainSubscriber.onComplete();
    } catch (Throwable t) {
      LOG.error(
          mainSubscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onComplete.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  // package-private because it can be invoked by the publisher if the subscription handshake
  // process fails.
  void doOnError(@NonNull Throwable error) {
    try {
      // Then we signal the error downstream, as per rules 1.2 and 1.4.
      mainSubscriber.onError(error);
    } catch (Throwable t) {
      t.addSuppressed(error);
      LOG.error(
          mainSubscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  private void clear() {
    // We don't need these pages anymore and should not hold references
    // to them.
    pages.clear();
    // As per 3.13, Subscription.cancel() MUST request the Publisher to
    // eventually drop any references to the corresponding subscriber.
    // Our own publishers do not keep references to this subscription,
    // but downstream processors might do so, which is why we need to
    // defensively clear the subscriber reference when we are done.
    mainSubscriber = null;
    columnDefinitionsSubscriber = null;
    executionInfosSubscriber = null;
    wasAppliedSubscriber = null;
  }

  /**
   * Converts the received result object into a {@link Page}.
   *
   * @param rs the result object to convert.
   * @return a new page.
   */
  @NonNull
  private Page<ResultSetT> toPage(@NonNull ResultSetT rs) {
    ExecutionInfo executionInfo = rs.getExecutionInfo();
    Iterator<ReactiveRow> results =
        Iterators.transform(
            rs.currentPage().iterator(),
            row -> new DefaultReactiveRow(Objects.requireNonNull(row), executionInfo));
    return new Page<>(results, rs.hasMorePages() ? rs::fetchNextPage : null);
  }

  /** Converts the given error into a {@link Page}, containing the error as its only element. */
  @NonNull
  private Page<ResultSetT> toErrorPage(@NonNull Throwable t) {
    return new Page<>(Iterators.singletonIterator(t), null);
  }

  /**
   * A page object comprises an iterator over the page's results, and a future pointing to the next
   * page (or {@code null}, if it's the last page).
   */
  static class Page<ResultSetT extends AsyncPagingIterable<Row, ResultSetT>> {

    @NonNull final Iterator<?> iterator;

    // A pointer to the next page, or null if this is the last page.
    @Nullable final Callable<CompletionStage<ResultSetT>> nextPage;

    /** called only from start() */
    Page(@NonNull Callable<CompletionStage<ResultSetT>> nextPage) {
      this.iterator = Collections.emptyIterator();
      this.nextPage = nextPage;
    }

    Page(@NonNull Iterator<?> iterator, @Nullable Callable<CompletionStage<ResultSetT>> nextPage) {
      this.iterator = iterator;
      this.nextPage = nextPage;
    }

    boolean hasMorePages() {
      return nextPage != null;
    }

    @NonNull
    CompletionStage<ResultSetT> fetchNextPage() {
      try {
        return Objects.requireNonNull(nextPage).call();
      } catch (Exception e) {
        // This is a synchronous failure in the driver.
        // It can happen in rare cases when the driver throws an exception instead of returning a
        // failed future; e.g. if someone tries to execute a continuous paging request but the
        // protocol version in use does not support it.
        // We treat it as a failed future.
        return CompletableFutures.failedFuture(e);
      }
    }

    boolean hasMoreRows() {
      return iterator.hasNext();
    }

    @NonNull
    Object nextRow() {
      return iterator.next();
    }
  }
}
