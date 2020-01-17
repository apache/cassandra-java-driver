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

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveRow;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.reactivex.Flowable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class DefaultReactiveResultSetTckTest extends PublisherVerification<ReactiveRow> {

  public DefaultReactiveResultSetTckTest() {
    super(new TestEnvironment());
  }

  @Override
  public Publisher<ReactiveRow> createPublisher(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    return new DefaultReactiveResultSet(() -> createResults(effective));
  }

  @Override
  public Publisher<ReactiveRow> createFailedPublisher() {
    DefaultReactiveResultSet publisher = new DefaultReactiveResultSet(() -> createResults(1));
    // Since our publisher does not support multiple
    // subscriptions, we use that to create a failed publisher.
    publisher.subscribe(new TestSubscriber<>());
    return publisher;
  }

  private static CompletableFuture<AsyncResultSet> createResults(int elements) {
    CompletableFuture<AsyncResultSet> previous = null;
    if (elements > 0) {
      // create pages of 5 elements each to exercise pagination
      List<Integer> pages =
          Flowable.range(0, elements).buffer(5).map(List::size).toList().blockingGet();
      Collections.reverse(pages);
      for (Integer size : pages) {
        CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
        future.complete(new MockAsyncResultSet(size, previous));
        previous = future;
      }
    } else {
      previous = new CompletableFuture<>();
      previous.complete(new MockAsyncResultSet(0, null));
    }
    return previous;
  }
}
