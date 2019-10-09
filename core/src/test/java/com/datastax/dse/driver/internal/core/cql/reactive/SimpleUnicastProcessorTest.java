/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.reactive;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class SimpleUnicastProcessorTest {

  /** Test for JAVA-2387. */
  @Test
  public void should_propagate_upstream_signals_when_downstream_already_subscribed() {
    // given
    SimpleUnicastProcessor<Integer> processor = new SimpleUnicastProcessor<>();
    TestSubscriber<Integer> subscriber = new TestSubscriber<>();
    // when
    processor.subscribe(subscriber); // subscription happens before signals arrive
    processor.onNext(1);
    processor.onComplete();
    subscriber.awaitTermination();
    // then
    assertThat(subscriber.getElements()).hasSize(1).containsExactly(1);
    assertThat(subscriber.getError()).isNull();
  }

  @Test
  public void should_delay_upstream_signals_until_downstream_is_subscribed() {
    // given
    SimpleUnicastProcessor<Integer> processor = new SimpleUnicastProcessor<>();
    TestSubscriber<Integer> subscriber = new TestSubscriber<>();
    // when
    processor.onNext(1);
    processor.onComplete();
    processor.subscribe(subscriber); // subscription happens after signals arrive
    subscriber.awaitTermination();
    // then
    assertThat(subscriber.getElements()).hasSize(1).containsExactly(1);
    assertThat(subscriber.getError()).isNull();
  }
}
