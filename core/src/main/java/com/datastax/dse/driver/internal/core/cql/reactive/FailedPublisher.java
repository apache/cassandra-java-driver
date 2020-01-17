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
