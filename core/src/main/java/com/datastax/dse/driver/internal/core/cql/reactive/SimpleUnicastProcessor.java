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
package com.datastax.dse.driver.internal.core.cql.reactive;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple {@link Processor} that receives items form an upstream publisher, stores them in an
 * internal queue, then serves them to one single downstream subscriber. It does not support
 * multiple subscriptions.
 *
 * <p>Implementation note: this class is intended to serve as the common implementation for all
 * secondary publishers exposed by the driver's reactive API, and in particular, for publishers of
 * query metadata objects. Since such publishers are not critical, and usually only publish a
 * handful of items, this implementation favors simplicity over efficiency (in particular, it uses
 * an unbounded linked queue, but in practice there is no risk that this queue could grow
 * uncontrollably).
 *
 * @param <ElementT> The type of elements received and emitted by this processor.
 */
public class SimpleUnicastProcessor<ElementT>
    implements Processor<ElementT, ElementT>, Subscription {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleUnicastProcessor.class);

  private static final Object ON_COMPLETE = new Object();

  private final Queue<Object> queue = new ConcurrentLinkedDeque<>();

  private final AtomicBoolean once = new AtomicBoolean(false);

  private final AtomicInteger draining = new AtomicInteger(0);

  private final AtomicLong requested = new AtomicLong(0);

  private volatile Subscriber<? super ElementT> subscriber;

  private volatile boolean cancelled;

  @Override
  public void subscribe(Subscriber<? super ElementT> subscriber) {
    // As per rule 1.9, we need to throw an NPE if subscriber is null
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // As per rule 1.11, this publisher supports only one subscriber.
    if (once.compareAndSet(false, true)) {
      this.subscriber = subscriber;
      try {
        subscriber.onSubscribe(this);
      } catch (Throwable t) {
        // As per rule 2.13: In the case that this rule is violated,
        // any associated Subscription to the Subscriber MUST be considered as
        // cancelled, and the caller MUST raise this error condition in a fashion
        // that is adequate for the runtime environment.
        doOnError(
            new IllegalStateException(
                subscriber
                    + " violated the Reactive Streams rule 2.13 by throwing an exception from onSubscribe.",
                t));
      }
    } else {
      subscriber.onSubscribe(EmptySubscription.INSTANCE);
      subscriber.onError(
          new IllegalStateException("This publisher does not support multiple subscriptions"));
    }
    // As per 2.13, this method must return normally (i.e. not throw)
  }

  @Override
  public void onSubscribe(Subscription s) {
    // no-op
  }

  @Override
  public void onNext(ElementT value) {
    if (!cancelled) {
      queue.offer(value);
      drain();
    }
  }

  @Override
  public void onError(Throwable error) {
    if (!cancelled) {
      queue.offer(error);
      drain();
    }
  }

  @Override
  public void onComplete() {
    if (!cancelled) {
      queue.offer(ON_COMPLETE);
      drain();
    }
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
                subscriber
                    + " violated the Reactive Streams rule 3.9 by requesting a non-positive number of elements."));
      } else {
        // As per rule 3.17, when demand overflows Long.MAX_VALUE
        // it can be treated as "effectively unbounded"
        ReactiveOperators.addCap(requested, n);
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

  @SuppressWarnings("ConditionalBreakInInfiniteLoop")
  private void drain() {
    if (draining.getAndIncrement() != 0) {
      return;
    }
    int missed = 1;
    for (; ; ) {
      // Note: when termination is detected inside this loop,
      // we MUST call clear() manually.
      long requested = this.requested.get();
      long emitted = 0L;
      while (requested != emitted) {
        if (cancelled) {
          clear();
          return;
        }
        Object t = queue.poll();
        if (t == null) {
          break;
        }
        if (t instanceof Throwable) {
          Throwable error = (Throwable) t;
          doOnError(error);
          clear();
          return;
        } else if (t == ON_COMPLETE) {
          doOnComplete();
          clear();
          return;
        } else {
          @SuppressWarnings("unchecked")
          ElementT item = (ElementT) t;
          doOnNext(item);
          emitted++;
        }
      }
      if (cancelled) {
        clear();
        return;
      }
      if (emitted != 0) {
        // if any item was emitted, adjust the requested field
        ReactiveOperators.subCap(this.requested, emitted);
      }
      // if another thread tried to call drain() while we were busy,
      // then we should do another drain round.
      missed = draining.addAndGet(-missed);
      if (missed == 0) {
        break;
      }
    }
  }

  private void doOnNext(@NonNull ElementT result) {
    try {
      subscriber.onNext(result);
    } catch (Throwable t) {
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onNext.",
          t);
      cancel();
    }
  }

  private void doOnComplete() {
    try {
      // Then we signal onComplete as per rules 1.2 and 1.5
      subscriber.onComplete();
    } catch (Throwable t) {
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onComplete.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  private void doOnError(@NonNull Throwable error) {
    try {
      // Then we signal the error downstream, as per rules 1.2 and 1.4.
      subscriber.onError(error);
    } catch (Throwable t) {
      t.addSuppressed(error);
      LOG.error(
          subscriber
              + " violated the Reactive Streams rule 2.13 by throwing an exception from onError.",
          t);
    }
    // We need to consider this Subscription as cancelled as per rule 1.6
    cancel();
  }

  private void clear() {
    // We don't need the elements anymore and should not hold references
    // to them.
    queue.clear();
    // As per 3.13, Subscription.cancel() MUST request the Publisher to
    // eventually drop any references to the corresponding subscriber.
    // Our own publishers do not keep references to this subscription,
    // but downstream processors might do so, which is why we need to
    // defensively clear the subscriber reference when we are done.
    subscriber = null;
  }
}
