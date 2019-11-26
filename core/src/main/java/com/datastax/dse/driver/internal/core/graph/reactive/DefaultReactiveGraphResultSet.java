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
package com.datastax.dse.driver.internal.core.graph.reactive;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphNode;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.internal.core.cql.reactive.EmptySubscription;
import com.datastax.dse.driver.internal.core.cql.reactive.SimpleUnicastProcessor;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import net.jcip.annotations.ThreadSafe;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

@ThreadSafe
public class DefaultReactiveGraphResultSet implements ReactiveGraphResultSet {

  private final Callable<CompletionStage<AsyncGraphResultSet>> firstPage;

  private final AtomicBoolean alreadySubscribed = new AtomicBoolean(false);

  private final SimpleUnicastProcessor<ExecutionInfo> executionInfosPublisher =
      new SimpleUnicastProcessor<>();

  public DefaultReactiveGraphResultSet(Callable<CompletionStage<AsyncGraphResultSet>> firstPage) {
    this.firstPage = firstPage;
  }

  @Override
  public void subscribe(@NonNull Subscriber<? super ReactiveGraphNode> subscriber) {
    // As per rule 1.9, we need to throw an NPE if subscriber is null
    Objects.requireNonNull(subscriber, "Subscriber cannot be null");
    // As per rule 1.11, this publisher is allowed to support only one subscriber.
    if (alreadySubscribed.compareAndSet(false, true)) {
      ReactiveGraphResultSetSubscription subscription =
          new ReactiveGraphResultSetSubscription(subscriber, executionInfosPublisher);
      try {
        subscriber.onSubscribe(subscription);
        // must be done after onSubscribe
        subscription.start(firstPage);
      } catch (Throwable t) {
        // As per rule 2.13: In the case that this rule is violated,
        // any associated Subscription to the Subscriber MUST be considered as
        // cancelled, and the caller MUST raise this error condition in a fashion
        // that is adequate for the runtime environment.
        subscription.doOnError(
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

  @NonNull
  @Override
  public Publisher<? extends ExecutionInfo> getExecutionInfos() {
    return executionInfosPublisher;
  }
}
