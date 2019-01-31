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
package com.datastax.oss.driver.internal.core.time;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.fail;
import static org.mockito.Mockito.when;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

public class AtomicTimestampGeneratorTest extends MonotonicTimestampGeneratorTestBase {
  @Override
  protected MonotonicTimestampGenerator newInstance(Clock clock) {
    return new AtomicTimestampGenerator(clock, context);
  }

  @Test
  public void should_share_timestamps_across_all_threads() throws Exception {
    // Prepare to generate 1000 timestamps with the clock frozen at 1
    OngoingStubbing<Long> stub = when(clock.currentTimeMicros());
    for (int i = 0; i < 1000; i++) {
      stub = stub.thenReturn(1L);
    }

    MonotonicTimestampGenerator generator = newInstance(clock);

    final int testThreadsCount = 2;
    assertThat(1000 % testThreadsCount).isZero();

    final SortedSet<Long> allTimestamps = new ConcurrentSkipListSet<Long>();
    ExecutorService executor = Executors.newFixedThreadPool(testThreadsCount);
    for (int i = 0; i < testThreadsCount; i++) {
      executor.submit(
          () -> {
            for (int j = 0; j < 1000 / testThreadsCount; j++) {
              allTimestamps.add(generator.next());
            }
          });
    }
    executor.shutdown();
    if (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
      fail("Expected executor to shut down cleanly");
    }

    assertThat(allTimestamps).hasSize(1000);
    assertThat(allTimestamps.first()).isEqualTo(1);
    assertThat(allTimestamps.last()).isEqualTo(1000);
  }
}
