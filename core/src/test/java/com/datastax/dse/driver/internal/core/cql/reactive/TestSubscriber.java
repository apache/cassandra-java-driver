/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
  private Subscription subscription;
  private Throwable error;

  @Override
  public void onSubscribe(Subscription s) {
    if (subscription != null) {
      fail("already subscribed");
    }
    subscription = s;
    s.request(Long.MAX_VALUE);
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
  }
}
