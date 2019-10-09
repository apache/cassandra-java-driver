/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import java.util.Objects;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} that immediately signals the error passed at instantiation to all its
 * subscribers.
 */
public class FailedPublisher<ElementT> implements Publisher<ElementT> {

  protected final Throwable error;

  public FailedPublisher(Throwable error) {
    this.error = error;
  }

  @Override
  public void subscribe(Subscriber<? super ElementT> subscriber) {
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // Per rule 1.9, we need to call onSubscribe before any other signal. Pass a dummy
    // subscription since we know it will never be used.
    subscriber.onSubscribe(EmptySubscription.INSTANCE);
    // Signal the error to the subscriber right away. This is safe to do because per rule 2.10,
    // a Subscriber MUST be prepared to receive an onError signal without a preceding
    // Subscription.request(long n) call.
    // Also, per rule 2.13: onError MUST return normally except when any provided parameter
    // is null (which is not the case here); so we don't need care about catching errors here.
    subscriber.onError(error);
  }
}
