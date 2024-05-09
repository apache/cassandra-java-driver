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

import com.datastax.oss.driver.internal.core.util.concurrent.ScheduledTaskCapturingEventLoop.CapturedTask;
import io.netty.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

public class ScheduledTaskCapturingEventLoopTest {

  @Test
  public void should_capture_task_and_let_test_complete_it_manually() {
    ScheduledTaskCapturingEventLoop eventLoop = new ScheduledTaskCapturingEventLoop(null);
    final AtomicBoolean ran = new AtomicBoolean();
    ScheduledFuture<?> future = eventLoop.schedule(() -> ran.set(true), 1, TimeUnit.NANOSECONDS);

    assertThat(future.isDone()).isFalse();
    assertThat(future.isCancelled()).isFalse();
    assertThat(ran.get()).isFalse();

    CapturedTask<?> task = eventLoop.nextTask();
    assertThat(task.getInitialDelay(TimeUnit.NANOSECONDS)).isEqualTo(1);

    task.run();

    assertThat(future.isDone()).isTrue();
    assertThat(future.isCancelled()).isFalse();
    assertThat(ran.get()).isTrue();
  }

  @Test
  public void should_let_tested_code_cancel_future() {
    ScheduledTaskCapturingEventLoop eventLoop = new ScheduledTaskCapturingEventLoop(null);
    final AtomicBoolean ran = new AtomicBoolean();
    ScheduledFuture<?> future = eventLoop.schedule(() -> ran.set(true), 1, TimeUnit.NANOSECONDS);

    assertThat(future.isDone()).isFalse();
    assertThat(future.isCancelled()).isFalse();
    assertThat(ran.get()).isFalse();

    future.cancel(true);

    assertThat(future.isDone()).isTrue();
    assertThat(future.isCancelled()).isTrue();
    assertThat(ran.get()).isFalse();
  }
}
