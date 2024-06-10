/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
