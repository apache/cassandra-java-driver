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
package com.datastax.oss.driver.internal.core.session.throttling;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class RateLimitingRequestThrottlerTest {

  private static final long ONE_HUNDRED_MILLISECONDS =
      TimeUnit.NANOSECONDS.convert(100, TimeUnit.MILLISECONDS);
  private static final long TWO_HUNDRED_MILLISECONDS =
      TimeUnit.NANOSECONDS.convert(200, TimeUnit.MILLISECONDS);
  private static final long TWO_SECONDS = TimeUnit.NANOSECONDS.convert(2, TimeUnit.SECONDS);

  // Note: we trigger scheduled task manually, so this is for verification purposes only, it doesn't
  // need to be consistent with the actual throttling rate.
  private static final Duration DRAIN_INTERVAL = Duration.ofMillis(10);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;
  @Mock private NettyOptions nettyOptions;
  @Mock private EventLoopGroup adminGroup;

  private ScheduledTaskCapturingEventLoop adminExecutor;
  private SettableNanoClock clock = new SettableNanoClock();

  private RateLimitingRequestThrottler throttler;

  @Before
  public void setup() {
    when(context.getConfig()).thenReturn(config);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);

    when(defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_REQUESTS_PER_SECOND))
        .thenReturn(5);
    when(defaultProfile.getInt(DefaultDriverOption.REQUEST_THROTTLER_MAX_QUEUE_SIZE))
        .thenReturn(10);

    // Set to match the time to reissue one permit. Although it does not matter in practice, since
    // the executor is mocked and we trigger tasks manually.
    when(defaultProfile.getDuration(DefaultDriverOption.REQUEST_THROTTLER_DRAIN_INTERVAL))
        .thenReturn(DRAIN_INTERVAL);

    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminGroup);
    adminExecutor = new ScheduledTaskCapturingEventLoop(adminGroup);
    when(adminGroup.next()).thenReturn(adminExecutor);

    throttler = new RateLimitingRequestThrottler(context, clock);
  }

  /** Note: the throttler starts with 1 second worth of permits, so at t=0 we have 5 available. */
  @Test
  public void should_start_immediately_when_under_capacity() {
    // Given
    MockThrottled request = new MockThrottled();

    // When
    throttler.register(request);

    // Then
    assertThatStage(request.started).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    assertThat(throttler.getStoredPermits()).isEqualTo(4);
    assertThat(throttler.getQueue()).isEmpty();
  }

  @Test
  public void should_allow_new_request_when_under_rate() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getStoredPermits()).isEqualTo(0);

    // When
    clock.add(TWO_HUNDRED_MILLISECONDS);
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThatStage(request.started).isSuccess(wasDelayed -> assertThat(wasDelayed).isFalse());
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).isEmpty();
  }

  @Test
  public void should_enqueue_when_over_rate() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getStoredPermits()).isEqualTo(0);

    // When
    // (do not advance time)
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThatStage(request.started).isNotDone();
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).containsExactly(request);

    ScheduledTaskCapturingEventLoop.CapturedTask<?> task = adminExecutor.nextTask();
    assertThat(task).isNotNull();
    assertThat(task.getInitialDelay(TimeUnit.NANOSECONDS)).isEqualTo(DRAIN_INTERVAL.toNanos());
  }

  @Test
  public void should_reject_when_queue_is_full() {
    // Given
    for (int i = 0; i < 15; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).hasSize(10);

    // When
    clock.add(TWO_HUNDRED_MILLISECONDS); // even if time has passed, queued items have priority
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThatStage(request.started)
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
    assertThatStage(queued2.started).isNotDone();
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).containsExactly(queued2);
  }

  @Test
  public void should_dequeue_when_draining_task_runs() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }

    MockThrottled queued1 = new MockThrottled();
    throttler.register(queued1);
    assertThatStage(queued1.started).isNotDone();
    MockThrottled queued2 = new MockThrottled();
    throttler.register(queued2);
    assertThatStage(queued2.started).isNotDone();
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).hasSize(2);

    ScheduledTaskCapturingEventLoop.CapturedTask<?> task = adminExecutor.nextTask();
    assertThat(task).isNotNull();
    assertThat(task.getInitialDelay(TimeUnit.NANOSECONDS)).isEqualTo(DRAIN_INTERVAL.toNanos());

    // When
    // (do not advance clock => no new permits)
    task.run();

    // Then
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).containsExactly(queued1, queued2);
    // task reschedules itself since it did not empty the queue
    task = adminExecutor.nextTask();
    assertThat(task).isNotNull();
    assertThat(task.getInitialDelay(TimeUnit.NANOSECONDS)).isEqualTo(DRAIN_INTERVAL.toNanos());

    // When
    clock.add(TWO_HUNDRED_MILLISECONDS); // 1 extra permit issued
    task.run();

    // Then
    assertThatStage(queued1.started).isSuccess(wasDelayed -> assertThat(wasDelayed).isTrue());
    assertThatStage(queued2.started).isNotDone();
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).containsExactly(queued2);
    // task reschedules itself since it did not empty the queue
    task = adminExecutor.nextTask();
    assertThat(task).isNotNull();
    assertThat(task.getInitialDelay(TimeUnit.NANOSECONDS)).isEqualTo(DRAIN_INTERVAL.toNanos());

    // When
    clock.add(TWO_HUNDRED_MILLISECONDS);
    task.run();

    // Then
    assertThatStage(queued2.started).isSuccess(wasDelayed -> assertThat(wasDelayed).isTrue());
    assertThat(throttler.getStoredPermits()).isEqualTo(0);
    assertThat(throttler.getQueue()).isEmpty();
    assertThat(adminExecutor.nextTask()).isNull();
  }

  @Test
  public void should_store_new_permits_up_to_threshold() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getStoredPermits()).isEqualTo(0);

    // When
    clock.add(TWO_SECONDS); // should store at most 1 second worth of permits

    // Then
    // acquire to trigger the throttler to update its permits
    throttler.register(new MockThrottled());
    assertThat(throttler.getStoredPermits()).isEqualTo(4);
  }

  /**
   * Ensure that permits are still created if we try to acquire faster than the minimal interval to
   * create one permit. In an early version of the code there was a bug where we would reset the
   * elapsed time on each acquisition attempt, and never regenerate permits.
   */
  @Test
  public void should_keep_accumulating_time_if_no_permits_created() {
    // Given
    for (int i = 0; i < 5; i++) {
      throttler.register(new MockThrottled());
    }
    assertThat(throttler.getStoredPermits()).isEqualTo(0);

    // When
    clock.add(ONE_HUNDRED_MILLISECONDS);

    // Then
    MockThrottled queued = new MockThrottled();
    throttler.register(queued);
    assertThatStage(queued.started).isNotDone();

    // When
    clock.add(ONE_HUNDRED_MILLISECONDS);
    adminExecutor.nextTask().run();

    // Then
    assertThatStage(queued.started).isSuccess(wasDelayed -> assertThat(wasDelayed).isTrue());
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
      assertThatStage(request.started).isNotDone();
      enqueued.add(request);
    }

    // When
    throttler.close();

    // Then
    for (MockThrottled request : enqueued) {
      assertThatStage(request.started)
          .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
    }

    // When
    MockThrottled request = new MockThrottled();
    throttler.register(request);

    // Then
    assertThatStage(request.started)
        .isFailed(error -> assertThat(error).isInstanceOf(RequestThrottlingException.class));
  }
}
