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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link SpeculativeExecutionPolicy} that schedules a given number of speculative executions, separated by a fixed delay.
 */
public class ConstantSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final int maxSpeculativeExecutions;
    private final long constantDelayMillis;

    /**
     * Builds a new instance.
     *
     * @param constantDelayMillis      the delay between each speculative execution. Must be strictly positive.
     * @param maxSpeculativeExecutions the number of speculative executions. Must be strictly positive.
     * @throws IllegalArgumentException if one of the arguments does not respect the preconditions above.
     */
    public ConstantSpeculativeExecutionPolicy(final long constantDelayMillis, final int maxSpeculativeExecutions) {
        Preconditions.checkArgument(constantDelayMillis > 0,
                "delay must be strictly positive (was %d)", constantDelayMillis);
        Preconditions.checkArgument(maxSpeculativeExecutions > 0,
                "number of speculative executions must be strictly positive (was %d)", maxSpeculativeExecutions);
        this.constantDelayMillis = constantDelayMillis;
        this.maxSpeculativeExecutions = maxSpeculativeExecutions;
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return new SpeculativeExecutionPlan() {
            private final AtomicInteger remaining = new AtomicInteger(maxSpeculativeExecutions);

            @Override
            public long nextExecution(Host lastQueried) {
                return (remaining.getAndDecrement() > 0) ? constantDelayMillis : -1;
            }
        };
    }

    @Override
    public void init(Cluster cluster) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
