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
package com.datastax.oss.driver.internal.core.session.throttling;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConcurrencyLimitingRequestThrottlerTest {

  @Mock private DriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;

  private ConcurrencyLimitingRequestThrottler throttler;

  @Before
  public void setup() {
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);

    when(defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_CONCURRENT_REQUESTS))
        .thenReturn(5);
    when(defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE))
        .thenReturn(10);

    throttler = new ConcurrencyLimitingRequestThrottler(context);
  }

  @Test
  public void should_start_immediately_when_under_capacity() {
    // Given
    MockThrottled request = new MockThrottled();

    // When
    throttler.register(request);

    // Then
    assertThatStage(request.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    assertThat(throttler.getConcurrentRequests()).isEqualTo(1);
    assertThat(throttler.getQueue()).isEmpty();
  }

  @Test
  public void should_allow_new_request_when_active_one_succeeds() {
    should_allow_new_request_when_active_one_completes(throttler::signalSuccess);
  }

  @Test
  public void should_allow_new_request_when_active_one_fails() {
    should_allow_new_request_when_active_one_completes(
        request -> throttler.signalError(request, new RuntimeException("mock error")));
  }

  @Test
  public void should_allow_new_request_when_active_one_times_out() {
    should_allow_new_request_when_active_one_completes(throttler::signalTimeout);
  }

  @Test
  public void should_allow_new_request_when_active_one_canceled() {
    should_allow_new_request_when_active_one_completes(throttler::signalCancel);
  }

  private void should_allow_new_request_when_active_one_completes(
      Consumer<Throttled> completeCallback) {
    // Given
    MockThrottled first = new MockThrottled();
    throttler.register(first);
    assertThatStage(first.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    for (int i = 0; i < 4; i++) { // fill to capacity
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).isEmpty();

    // When
    completeCallback.accept(first);
    assertThat(throttler.getConcurrentRequests()).isEqualTo(4);
    assertThat(throttler.getQueue()).isEmpty();
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThatStage(incoming.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).isEmpty();
  }

  @Test
  public void should_enqueue_when_over_capacity() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).isEmpty();

    // When
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThatStage(incoming.ended).isNotDone();
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).containsExactly(incoming);
  }

  @Test
  public void should_dequeue_when_active_succeeds() {
    should_dequeue_when_active_completes(throttler::signalSuccess);
  }

  @Test
  public void should_dequeue_when_active_fails() {
    should_dequeue_when_active_completes(
        request -> throttler.signalError(request, new RuntimeException("mock error")));
  }

  @Test
  public void should_dequeue_when_active_times_out() {
    should_dequeue_when_active_completes(throttler::signalTimeout);
  }

  private void should_dequeue_when_active_completes(Consumer<Throttled> completeCallback) {
    // Given
    MockThrottled first = new MockThrottled();
    throttler.register(first);
    assertThatStage(first.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    for (int i = 0; i < 4; i++) {
      throttler.register(new MockThrottled());
    }

    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);
    assertThatStage(incoming.ended).isNotDone();

    // When
    completeCallback.accept(first);

    // Then
    assertThatStage(incoming.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isTrue());
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).isEmpty();
  }

  @Test
  public void should_reject_when_queue_is_full() {
    // Given
    for (int i = 0; i < 15; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).hasSize(10);

    // When
    MockThrottled incoming = new MockThrottled();
    throttler.register(incoming);

    // Then
    assertThatStage(incoming.ended)
        .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
  }

  @Test
  public void should_remove_timed_out_request_from_queue() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    MockThrottled queued1 = new MockThrottled();
    throttler.register(queued1);
    MockThrottled queued2 = new MockThrottled();
    throttler.register(queued2);

    // When
    throttler.signalTimeout(queued1);

    // Then
    assertThatStage(queued2.ended).isNotDone();
    assertThat(throttler.getConcurrentRequests()).isEqualTo(5);
    assertThat(throttler.getQueue()).hasSize(1);
  }

  @Test
  public void should_reject_enqueued_when_closing() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    List<MockThrottled> enqueued = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      MockThrottled request = new MockThrottled();
      throttler.register(request);
      assertThatStage(request.ended).isNotDone();
      enqueued.add(request);
    }

    // When
    throttler.close();

    // Then
    for (MockThrottled request : enqueued) {
      assertThatStage(request.ended)
          .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
    }

    // When
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThatStage(request.ended)
        .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
  }

  @Test
  public void should_run_throttle_callbacks_concurrently() throws InterruptedException {
    // Given

    // a task is enqueued, which when in onThrottleReady, will stall latch countDown()ed
    // register() should automatically start onThrottleReady on same thread

    // start a parallel thread
    CountDownLatch firstRelease = new CountDownLatch(1);
    MockThrottled first = new MockThrottled(firstRelease);
    Runnable r =
        () -> {
          throttler.register(first);
          first.ended.toCompletableFuture().thenRun(() -> throttler.signalSuccess(first));
        };
    Thread t = new Thread(r);
    t.start();

    // wait for the registration threads to reach await state
    assertThatStage(first.started).isSuccess();
    assertThatStage(first.ended).isNotDone();

    // When
    // we concurrently submit a second shorter task
    MockThrottled second = new MockThrottled();
    // (on a second thread, so that we can join and force a timeout in case
    // registration is delayed)
    Thread t2 = new Thread(() -> throttler.register(second));
    t2.start();
    t2.join(1_000);

    // Then
    // registration will trigger callback, should complete ~immediately
    assertThatStage(second.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    // first should still be unfinished
    assertThatStage(first.started).isDone();
    assertThatStage(first.ended).isNotDone();
    // now finish, and verify
    firstRelease.countDown();
    assertThatStage(first.ended).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());

    t.join(1_000);
  }

  @Test
  public void should_enqueue_tasks_quickly_when_callbacks_blocked() throws InterruptedException {
    // Given

    // Multiple tasks are registered, up to the limit, and proceed into their
    // callback

    // start five parallel threads
    final int THREADS = 5;
    Thread[] threads = new Thread[THREADS];
    CountDownLatch[] latches = new CountDownLatch[THREADS];
    MockThrottled[] throttled = new MockThrottled[THREADS];
    for (int i = 0; i < threads.length; i++) {
      latches[i] = new CountDownLatch(1);
      final MockThrottled itThrottled = new MockThrottled(latches[i]);
      throttled[i] = itThrottled;
      threads[i] =
          new Thread(
              () -> {
                throttler.register(itThrottled);
                itThrottled
                    .ended
                    .toCompletableFuture()
                    .thenRun(() -> throttler.signalSuccess(itThrottled));
              });
      threads[i].start();
    }

    // wait for the registration threads to be launched
    // they are all waiting now
    for (int i = 0; i < throttled.length; i++) {
      assertThatStage(throttled[i].started).isSuccess();
      assertThatStage(throttled[i].ended).isNotDone();
    }

    // When
    // we concurrently submit another task
    MockThrottled last = new MockThrottled();
    throttler.register(last);

    // Then
    // registration will enqueue the callback, and it should not
    // take any time to proceed (ie: we should not be blocked)
    // and there should be an element in the queue
    assertThatStage(last.started).isNotDone();
    assertThatStage(last.ended).isNotDone();
    assertThat(throttler.getQueue()).containsExactly(last);

    // we still have not released, so old throttled threads should be waiting
    for (int i = 0; i < throttled.length; i++) {
      assertThatStage(throttled[i].started).isDone();
      assertThatStage(throttled[i].ended).isNotDone();
    }

    // now let us release ..
    for (int i = 0; i < latches.length; i++) {
      latches[i].countDown();
    }

    // .. and check everything finished up OK
    for (int i = 0; i < latches.length; i++) {
      assertThatStage(throttled[i].started).isSuccess();
      assertThatStage(throttled[i].ended).isSuccess();
    }

    // for good measure, we will also wait for the enqueued to complete
    assertThatStage(last.started).isSuccess();
    assertThatStage(last.ended).isSuccess();

    for (int i = 0; i < threads.length; i++) {
      threads[i].join(1_000);
    }
  }
}
