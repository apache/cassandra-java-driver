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
