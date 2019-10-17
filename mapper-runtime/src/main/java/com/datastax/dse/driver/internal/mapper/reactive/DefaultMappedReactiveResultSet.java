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
package com.datastax.dse.driver.internal.mapper.reactive;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.function.Function;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMappedReactiveResultSet<EntityT> implements MappedReactiveResultSet<EntityT> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMappedReactiveResultSet.class);

  private static final Subscription EMPTY_SUBSCRIPTION =
      new Subscription() {
        @Override
        public void request(long n) {}

        @Override
        public void cancel() {}
      };

  @NonNull private final ReactiveResultSet source;

  @NonNull private final Function<ReactiveRow, EntityT> mapper;

  public DefaultMappedReactiveResultSet(
      @NonNull ReactiveResultSet source, @NonNull Function<ReactiveRow, EntityT> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  @Override
  @NonNull
  public Publisher<? extends ColumnDefinitions> getColumnDefinitions() {
    return source.getColumnDefinitions();
  }

  @Override
  @NonNull
  public Publisher<? extends ExecutionInfo> getExecutionInfos() {
    return source.getExecutionInfos();
  }

  @Override
  @NonNull
  public Publisher<Boolean> wasApplied() {
    return source.wasApplied();
  }

  @Override
  public void subscribe(@NonNull Subscriber<? super EntityT> subscriber) {
    // As per rule 1.9, we need to throw an NPE if subscriber is null
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // As per rule 1.11, this publisher supports multiple subscribers in a unicast configuration,
    // as long as the source publisher does too.
    MappedReactiveResultSetSubscriber s = new MappedReactiveResultSetSubscriber(subscriber);
    try {
      source.subscribe(s);
    } catch (Throwable t) {
      // As per rule 1.9: subscribe MUST return normally. The only legal way to signal failure (or
      // reject the Subscriber) is by calling onError (after calling onSubscribe).
      s.cancel();
      IllegalStateException error =
          new IllegalStateException(
              "Publisher violated $1.9 by throwing an exception from subscribe.", t);
      LOG.error(error.getMessage(), error.getCause());
      // This may violate 1.9 since we cannot know if subscriber.onSubscribe was called or not.
      subscriber.onSubscribe(EMPTY_SUBSCRIPTION);
      subscriber.onError(error);
    }
    // As per 1.9, this method must return normally (i.e. not throw)
  }

  private class MappedReactiveResultSetSubscriber implements Subscriber<ReactiveRow>, Subscription {

    private volatile Subscriber<? super EntityT> downstreamSubscriber;
    private volatile Subscription upstreamSubscription;
    private volatile boolean terminated;

    MappedReactiveResultSetSubscriber(@NonNull Subscriber<? super EntityT> subscriber) {
      this.downstreamSubscriber = subscriber;
    }

    @Override
    public void onSubscribe(@NonNull Subscription subscription) {
      // As per rule 2.13, we need to throw NPE if the subscription is null
      Objects.requireNonNull(subscription, "Subscription cannot be null");
      // As per rule 2.12, Subscriber.onSubscribe MUST be called at most once for a given subscriber
      if (upstreamSubscription != null) {
        try {
          // Cancel the additional subscription
          subscription.cancel();
        } catch (Throwable t) {
          // As per rule 3.15, Subscription.cancel is not allowed to throw an exception; the only
          // thing we can do is log.
          LOG.error("Subscription violated $3.15 by throwing an exception from cancel.", t);
        }
      } else if (!terminated) {
        upstreamSubscription = subscription;
        try {
          downstreamSubscriber.onSubscribe(this);
        } catch (Throwable t) {
          // As per rule 2.13: In the case that this rule is violated,
          // any associated Subscription to the Subscriber MUST be considered as
          // cancelled...
          cancel();
          // ...and the caller MUST raise this error condition in a fashion that is "adequate for
          // the runtime environment" (we choose to log).
          LOG.error("Subscriber violated $2.13 by throwing an exception from onSubscribe.", t);
        }
      }
    }

    @Override
    public void onNext(@NonNull ReactiveRow row) {
      LOG.trace("Received onNext: {}", row);
      if (upstreamSubscription == null) {
        LOG.error("Publisher violated $1.09 by signalling onNext prior to onSubscribe.");
      } else if (!terminated) {
        Objects.requireNonNull(row, "Publisher violated $2.13 by emitting a null element");
        EntityT entity;
        try {
          entity = mapper.apply(row);
        } catch (Throwable t) {
          onError(t);
          return;
        }
        Objects.requireNonNull(entity, "Publisher violated $2.13 by generating a null entity");
        try {
          downstreamSubscriber.onNext(entity);
        } catch (Throwable t) {
          LOG.error("Subscriber violated $2.13 by throwing an exception from onNext.", t);
          cancel();
        }
      }
    }

    @Override
    public void onComplete() {
      LOG.trace("Received onComplete");
      if (upstreamSubscription == null) {
        LOG.error("Publisher violated $1.09 by signalling onComplete prior to onSubscribe.");
      } else if (!terminated) {
        try {
          downstreamSubscriber.onComplete();
        } catch (Throwable t) {
          LOG.error("Subscriber violated $2.13 by throwing an exception from onComplete.", t);
        }
        // We need to consider this Subscription as cancelled as per rule 1.6
        cancel();
      }
    }

    @Override
    public void onError(@NonNull Throwable error) {
      LOG.trace("Received onError", error);
      if (upstreamSubscription == null) {
        LOG.error("Publisher violated $1.09 by signalling onError prior to onSubscribe.");
      } else if (!terminated) {
        Objects.requireNonNull(error, "Publisher violated $2.13 by signalling a null error");
        try {
          downstreamSubscriber.onError(error);
        } catch (Throwable t) {
          t.addSuppressed(error);
          LOG.error("Subscriber violated $2.13 by throwing an exception from onError.", t);
        }
        // We need to consider this Subscription as cancelled as per rule 1.6
        cancel();
      }
    }

    @Override
    public void request(long n) {
      LOG.trace("Received request: {}", n);
      // As per 3.6: after the Subscription is cancelled, additional calls to request() MUST be
      // NOPs.
      // Implementation note: triggering onError() from below may break 1.3 because this method is
      // called by the subscriber thread, and it can race with the producer thread. But these
      // situations are already abnormal, so there is no point in trying to prevent the race
      // condition with locks.
      if (!terminated) {
        if (n <= 0) {
          // Validate request as per rule 3.9: While the subscription is not cancelled,
          // Subscription.request(long n) MUST signal onError with a
          // java.lang.IllegalArgumentException if the argument is <= 0.
          // The cause message SHOULD explain that non-positive request signals are illegal.
          onError(
              new IllegalArgumentException(
                  "Subscriber violated $3.9 by requesting a non-positive number of elements."));
        } else {
          try {
            upstreamSubscription.request(n);
          } catch (Throwable t) {
            // As per rule 3.16, Subscription.request is not allowed to throw
            IllegalStateException error =
                new IllegalStateException(
                    "Subscription violated $3.16 by throwing an exception from request.", t);
            onError(error);
          }
        }
      }
    }

    @Override
    public void cancel() {
      // As per 3.5: Subscription.cancel() MUST respect the responsiveness of its caller by
      // returning in a timely manner, MUST be idempotent and MUST be thread-safe.
      if (!terminated) {
        terminated = true;
        LOG.trace("Cancelling");
        // propagate cancellation, if we got a chance to subscribe to the upstream source
        if (upstreamSubscription != null) {
          upstreamSubscription.cancel();
        }
        // As per 3.13, Subscription.cancel() MUST request the Publisher to
        // eventually drop any references to the corresponding subscriber.
        downstreamSubscriber = null;
        upstreamSubscription = null;
      }
    }
  }
}
