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
import com.datastax.driver.core.Statement;

/**
 * Base class for tests that want to wrap a policy to add some instrumentation.
 * <p/>
 * NB: this is currently only used in tests, but could be provided as a convenience in the production code.
 */
public abstract class DelegatingSpeculativeExecutionPolicy implements SpeculativeExecutionPolicy {
    private final SpeculativeExecutionPolicy delegate;

    protected DelegatingSpeculativeExecutionPolicy(SpeculativeExecutionPolicy delegate) {
        this.delegate = delegate;
    }

    @Override
    public void init(Cluster cluster) {
        delegate.init(cluster);
    }

    @Override
    public SpeculativeExecutionPlan newPlan(String loggedKeyspace, Statement statement) {
        return delegate.newPlan(loggedKeyspace, statement);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
