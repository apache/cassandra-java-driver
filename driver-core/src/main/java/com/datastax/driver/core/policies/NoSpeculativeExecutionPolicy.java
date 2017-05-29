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

/**
 * A {@link SpeculativeExecutionPolicy} that never schedules speculative executions.
 */
public class NoSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {

    /**
     * The single instance (this class is stateless).
     */
    public static final NoSpeculativeExecutionPolicy INSTANCE = new NoSpeculativeExecutionPolicy();

    private static final SpeculativeExecutionPlan PLAN = new SpeculativeExecutionPlan() {
        @Override
        public long nextExecution(Host lastQueried) {
            return -1;
        }
    };

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return PLAN;
    }

    private NoSpeculativeExecutionPolicy() {
        // do nothing
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
