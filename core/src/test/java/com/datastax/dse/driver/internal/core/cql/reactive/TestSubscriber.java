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

import static org.assertj.core.api.Fail.fail;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class TestSubscriber<T> implements Subscriber<T> {

  private final List<T> elements = new ArrayList<>();
  private final CountDownLatch latch = new CountDownLatch(1);
  private final long demand;
  private Subscription subscription;
  private Throwable error;

  public TestSubscriber() {
    this.demand = Long.MAX_VALUE;
  }

  public TestSubscriber(long demand) {
    this.demand = demand;
  }

  @Override
  public void onSubscribe(Subscription s) {
    if (subscription != null) {
      fail("already subscribed");
    }
    subscription = s;
    subscription.request(demand);
  }

  @Override
  public void onNext(T t) {
    elements.add(t);
  }

  @Override
  public void onError(Throwable t) {
    error = t;
    latch.countDown();
  }

  @Override
  public void onComplete() {
    latch.countDown();
  }

  @Nullable
  public Throwable getError() {
    return error;
  }

  @NonNull
  public List<T> getElements() {
    return elements;
  }

  public void awaitTermination() {
    Uninterruptibles.awaitUninterruptibly(latch, 1, TimeUnit.MINUTES);
    if (latch.getCount() > 0) fail("subscriber not terminated");
  }
}
