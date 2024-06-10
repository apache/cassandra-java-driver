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
package com.datastax.oss.driver.internal.core.time;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static com.datastax.oss.driver.Assertions.fail;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class ThreadLocalTimestampGeneratorTest extends MonotonicTimestampGeneratorTestBase {
  @Override
  protected MonotonicTimestampGenerator newInstance(Clock clock) {
    return new ThreadLocalTimestampGenerator(clock, context);
  }

  @Test
  public void should_confine_timestamps_to_thread() throws Exception {
    final int testThreadsCount = 2;

    // Prepare to generate 1000 timestamps for each thread, with the clock frozen at 1
    OngoingStubbing<Long> stub = when(clock.currentTimeMicros());
    for (int i = 0; i < testThreadsCount * 1000; i++) {
      stub = stub.thenReturn(1L);
    }

    MonotonicTimestampGenerator generator = newInstance(clock);

    List<CompletionStage<Void>> futures = new CopyOnWriteArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(testThreadsCount);
    for (int i = 0; i < testThreadsCount; i++) {
      executor.submit(
          () -> {
            try {
              for (long l = 1; l <= 1000; l++) {
                assertThat(generator.next()).isEqualTo(l);
              }
              futures.add(CompletableFuture.completedFuture(null));
            } catch (Throwable t) {
              futures.add(CompletableFutures.failedFuture(t));
            }
          });
    }
    executor.shutdown();
    if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
      fail("Expected executor to shut down cleanly");
    }

    assertThat(futures).hasSize(testThreadsCount);
    for (CompletionStage<Void> future : futures) {
      assertThatStage(future).isSuccess();
    }
  }
}
