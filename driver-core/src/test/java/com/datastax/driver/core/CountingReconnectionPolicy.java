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

import com.datastax.driver.core.policies.ReconnectionPolicy;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reconnection policy that tracks how many times its schedule has been invoked.
 */
public class CountingReconnectionPolicy implements ReconnectionPolicy {
    public final AtomicInteger count = new AtomicInteger();
    private final ReconnectionPolicy childPolicy;

    public CountingReconnectionPolicy(ReconnectionPolicy childPolicy) {
        this.childPolicy = childPolicy;
    }

    @Override
    public ReconnectionSchedule newSchedule() {
        return new CountingSchedule(childPolicy.newSchedule());
    }

    class CountingSchedule implements ReconnectionSchedule {
        private final ReconnectionSchedule childSchedule;

        public CountingSchedule(ReconnectionSchedule childSchedule) {
            this.childSchedule = childSchedule;
        }

        @Override
        public long nextDelayMs() {
            count.incrementAndGet();
            return childSchedule.nextDelayMs();
        }
    }

    @Override
    public void init(Cluster cluster) {
    }

    @Override
    public void close() {
        childPolicy.close();
    }

}
