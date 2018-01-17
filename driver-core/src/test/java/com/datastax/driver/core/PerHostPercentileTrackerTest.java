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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class PerHostPercentileTrackerTest
        extends PercentileTrackerTest<PerHostPercentileTracker.Builder, PerHostPercentileTracker> {

    @Test(groups = "unit")
    public void should_track_measurements_by_host() {
        // given - a per host percentile tracker.
        Cluster cluster0 = mock(Cluster.class);
        PerHostPercentileTracker tracker = builder()
                .withInterval(1, TimeUnit.SECONDS)
                .withMinRecordedValues(100).build();
        tracker.onRegister(cluster0);

        List<Host> hosts = Lists.newArrayList(mock(Host.class), mock(Host.class), mock(Host.class));
        List<Statement> statements = Lists.newArrayList(mock(Statement.class), mock(Statement.class));
        List<Exception> exceptions = Lists.newArrayList(new Exception(), null, new ReadTimeoutException(ConsistencyLevel.ANY, 1, 1, true), null, null);

        // when - recording latencies over a linear progression with varying hosts, statements and exceptions.
        for (int i = 0; i < 100; i++) {
            tracker.update(hosts.get(0),
                    statements.get(i % statements.size()),
                    exceptions.get(i % exceptions.size()),
                    TimeUnit.NANOSECONDS.convert(i + 1, TimeUnit.MILLISECONDS));

            tracker.update(hosts.get(1),
                    statements.get(i % statements.size()),
                    exceptions.get(i % exceptions.size()),
                    TimeUnit.NANOSECONDS.convert((i + 1) * 2, TimeUnit.MILLISECONDS));

            tracker.update(hosts.get(2),
                    statements.get(i % statements.size()),
                    exceptions.get(i % exceptions.size()),
                    TimeUnit.NANOSECONDS.convert((i + 1) * 3, TimeUnit.MILLISECONDS));
        }
        Uninterruptibles.sleepUninterruptibly(2000, TimeUnit.MILLISECONDS);

        // then - the resulting tracker's percentiles should represent that linear progression for each host individually.
        // host0: (x percentile == x)
        // host1: (x percentile == 2x)
        // host2: (x percentile == 3x)
        for (int i = 1; i <= 99; i++) {
            assertThat(tracker.getLatencyAtPercentile(hosts.get(0), null, null, i)).isEqualTo(i);
            assertThat(tracker.getLatencyAtPercentile(hosts.get(1), null, null, i)).isEqualTo(i * 2);
            assertThat(tracker.getLatencyAtPercentile(hosts.get(2), null, null, i)).isEqualTo(i * 3);
        }
    }


    @Override
    public PerHostPercentileTracker.Builder builder() {
        return PerHostPercentileTracker.builder(defaultMaxLatency);
    }
}
