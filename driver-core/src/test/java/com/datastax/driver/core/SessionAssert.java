package com.datastax.driver.core;

import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class SessionAssert extends AbstractAssert<SessionAssert, SessionManager> {

    protected SessionAssert(Session actual) {
        // We are cheating a bit by casting, but this is the only implementation anyway
        super((SessionManager)actual, SessionAssert.class);
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
