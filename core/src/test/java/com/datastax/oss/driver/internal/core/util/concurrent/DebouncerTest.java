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
package com.datastax.oss.driver.internal.core.util.concurrent;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.ScheduledFuture;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DebouncerTest {

  private static final Duration DEFAULT_WINDOW = Duration.ofSeconds(1);
  private static final int DEFAULT_MAX_EVENTS = 10;

  @Mock private EventExecutor adminExecutor;
  @Mock private ScheduledFuture<?> scheduledFuture;
  private List<String> results;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(adminExecutor.inEventLoop()).thenReturn(true);
    when(adminExecutor.schedule(
            any(Runnable.class), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS)))
        .thenAnswer((i) -> scheduledFuture);
    results = new ArrayList<>();
  }

  private String coalesce(List<Integer> events) {
    return Joiner.on(",").join(events);
  }

  private void flush(String result) {
    results.add(result);
  }

  @Test
  public void should_flush_synchronously_if_window_is_zero() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, Duration.ZERO, DEFAULT_MAX_EVENTS);

    debouncer.receive(1);
    debouncer.receive(2);

    verify(adminExecutor, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    assertThat(results).containsExactly("1", "2");
  }

  @Test
  public void should_flush_synchronously_if_max_events_is_one() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, 1);

    debouncer.receive(1);
    debouncer.receive(2);

    verify(adminExecutor, never()).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

    assertThat(results).containsExactly("1", "2");
  }

  @Test
  public void should_debounce_after_time_window_if_no_other_event() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, DEFAULT_MAX_EVENTS);
    debouncer.receive(1);

    // a task should have been scheduled, run it
    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    verify(adminExecutor)
        .schedule(captor.capture(), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));
    captor.getValue().run();

    // the element should have been flushed
    assertThat(results).containsExactly("1");
  }

  @Test
  public void should_reset_time_window_when_new_event() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, DEFAULT_MAX_EVENTS);
    debouncer.receive(1);
    debouncer.receive(2);

    InOrder inOrder = inOrder(adminExecutor, scheduledFuture);

    // a first task should have been scheduled, and then cancelled
    inOrder
        .verify(adminExecutor)
        .schedule(any(Runnable.class), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));
    inOrder.verify(scheduledFuture).cancel(true);

    // a second task should have been scheduled, run it
    ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
    inOrder
        .verify(adminExecutor)
        .schedule(captor.capture(), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));
    captor.getValue().run();

    // both elements should have been flushed together
    assertThat(results).containsExactly("1,2");
  }

  @Test
  public void should_force_flush_after_max_events() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, DEFAULT_MAX_EVENTS);
    for (int i = 0; i < 10; i++) {
      debouncer.receive(i);
    }
    verify(adminExecutor, times(9))
        .schedule(any(Runnable.class), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));
    verify(scheduledFuture, times(9)).cancel(true);
    assertThat(results).containsExactly("0,1,2,3,4,5,6,7,8,9");
  }

  @Test
  public void should_cancel_next_flush_when_stopped() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, DEFAULT_MAX_EVENTS);

    debouncer.receive(1);
    verify(adminExecutor)
        .schedule(any(Runnable.class), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));

    debouncer.stop();
    verify(scheduledFuture).cancel(true);
  }

  @Test
  public void should_ignore_new_events_when_flushed() {
    Debouncer<Integer, String> debouncer =
        new Debouncer<>(
            adminExecutor, this::coalesce, this::flush, DEFAULT_WINDOW, DEFAULT_MAX_EVENTS);
    debouncer.stop();

    debouncer.receive(1);
    verify(adminExecutor, never())
        .schedule(any(Runnable.class), eq(DEFAULT_WINDOW.toNanos()), eq(TimeUnit.NANOSECONDS));
  }
}
