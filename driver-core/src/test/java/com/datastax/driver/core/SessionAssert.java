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

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionAssert extends AbstractAssert<SessionAssert, SessionManager> {

    protected SessionAssert(Session actual) {
        // We are cheating a bit by casting, but this is the only implementation anyway
        super((SessionManager) actual, SessionAssert.class);
    }

    public SessionAssert hasPoolFor(int hostNumber) {
        Host host = TestUtils.findHost(actual.cluster, hostNumber);
        assertThat(actual.pools.containsKey(host)).isTrue();
        return this;
    }

    public SessionAssert hasNoPoolFor(int hostNumber) {
        Host host = TestUtils.findHost(actual.cluster, hostNumber);
        assertThat(actual.pools.containsKey(host)).isFalse();
        return this;
    }
}
