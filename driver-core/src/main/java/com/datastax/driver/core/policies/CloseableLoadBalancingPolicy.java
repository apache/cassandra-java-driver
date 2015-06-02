/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

/**
 * A load balancing policy that wants to be notified at cluster shutdown.
 *
 * The only reason that this is separate from {@link LoadBalancingPolicy} is to avoid breaking binary compatibility at the
 * time this was introduced (2.0.7 / 2.1.3). It might be merged with the parent interface in a future major version.
 */
public interface CloseableLoadBalancingPolicy extends LoadBalancingPolicy {
    /**
     * Gets invoked at cluster shutdown.
     *
     * This gives the policy the opportunity to perform some cleanup, for instance stop threads that it might have started.
     */
    void close();
}
