/*
 * Copyright (C) 2012-2017 DataStax Inc.
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

public class CustomPercentileTrackerTest {

    @Test(groups = "unit")
    public void should_return_negative_value_when_key_cant_be_computed() {
        // given - A custom tracker that returns null for a specific host and keys by host otherwise.
        final Cluster cluster0 = mock(Cluster.class);
        final Host host0 = mock(Host.class);
        final Host host1 = mock(Host.class);

        PercentileTracker tracker = new PercentileTracker(1000, 3, 100, 50) {
            @Override
            protected Object computeKey(Host host, Statement statement, Exception exception) {
                if (host == host0) {
                    return host;
                } else {
                    return null;
                }
            }
        };
        tracker.onRegister(cluster0);

        List<Statement> statements = Lists.newArrayList(mock(Statement.class), mock(Statement.class));
        List<Exception> exceptions = Lists.newArrayList(new Exception(), null, new ReadTimeoutException(ConsistencyLevel.ANY, 1, 1, true), null, null);

        // when - recording latencies over a linear progression with varying hosts, statements and exceptions.
        for (int i = 0; i < 100; i++) {
            tracker.update(
                    host0,
                    statements.get(i % statements.size()),
                    exceptions.get(i % exceptions.size()), TimeUnit.NANOSECONDS.convert((i + 1) * 2, TimeUnit.MILLISECONDS));

            tracker.update(
                    host1,
                    statements.get(i % statements.size()),
                    exceptions.get(i % exceptions.size()), TimeUnit.NANOSECONDS.convert(i + 1, TimeUnit.MILLISECONDS));
        }
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);

        // then - host0 should return a linear progression (i*2) since it has a tracker.
        //        host1 should return -1 since it has no tracker since it has no key.
        for (int i = 1; i <= 99; i++) {
            assertThat(tracker.getLatencyAtPercentile(host0, null, null, i)).isEqualTo(i * 2);
            assertThat(tracker.getLatencyAtPercentile(host1, null, null, i)).isEqualTo(-1);
        }

    }
}
