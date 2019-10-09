/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import org.reactivestreams.Publisher;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public class SimpleUnicastProcessorTckTest extends PublisherVerification<Integer> {

  public SimpleUnicastProcessorTckTest() {
    super(new TestEnvironment());
  }

  @Override
  public Publisher<Integer> createPublisher(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    SimpleUnicastProcessor<Integer> processor = new SimpleUnicastProcessor<>();
    for (int i = 0; i < effective; i++) {
      processor.onNext(i);
    }
    processor.onComplete();
    return processor;
  }

  @Override
  public Publisher<Integer> createFailedPublisher() {
    SimpleUnicastProcessor<Integer> processor = new SimpleUnicastProcessor<>();
    // Since our publisher does not support multiple
    // subscriptions, we use that to create a failed publisher.
    processor.subscribe(new TestSubscriber<>());
    return processor;
  }
}
