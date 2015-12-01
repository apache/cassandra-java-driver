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
package com.datastax.driver.core;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.fail;
import org.assertj.core.api.AbstractAssert;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.policies.LoadBalancingPolicy;

public class HostAssert extends AbstractAssert<HostAssert, Host> {

    private final Cluster cluster;

    protected HostAssert(Host host) {
        this(host, null);
    }

    protected HostAssert(Host host, Cluster cluster) {
        super(host, HostAssert.class);
        this.cluster = cluster;
    }

    public HostAssert hasState(Host.State expected) {
        assertThat(actual.state).isEqualTo(expected);
        return this;
    }

    public HostAssert isUp() {
        assertThat(actual.isUp()).isTrue();
        return this;
    }

    public HostAssert isDown() {
        assertThat(actual.isUp()).isFalse();
        return this;
    }

    public HostAssert isAtDistance(HostDistance expected) {
        LoadBalancingPolicy loadBalancingPolicy = cluster.manager.loadBalancingPolicy();
        assertThat(loadBalancingPolicy.distance(actual)).isEqualTo(expected);
        return this;
    }

    public HostAssert isReconnectingFromDown() {
        assertThat(actual.getReconnectionAttemptFuture() != null && !actual.getReconnectionAttemptFuture().isDone())
            .isTrue();
        return this;
    }

    public HostAssert isInDatacenter(String datacenter) {
        assertThat(actual.getDatacenter()).isEqualTo(datacenter);
        return this;
    }

    public HostAssert isNotReconnectingFromDown() {
        assertThat(actual.getReconnectionAttemptFuture() != null && !actual.getReconnectionAttemptFuture().isDone())
            .isFalse();
        return this;
    }

    public HostAssert comesUpWithin(long duration, TimeUnit unit) {
        final CountDownLatch upSignal = new CountDownLatch(1);
        StateListener upListener = new StateListenerBase() {

            @Override
            public void onUp(Host host) {
                upSignal.countDown();
            }

            @Override
            public void onAdd(Host host) {
                // Special case, cassandra will sometimes not send an 'UP' topology change event
                // for a new node, because of this we also listen for add events.
                upSignal.countDown();
            }
        };
        cluster.register(upListener);
        try {
            // If the host is already up or if we receive the UP signal within given time
            if (actual.isUp() || upSignal.await(duration, unit)) {
                return this;
            }
        } catch (InterruptedException e) {
            fail("Got interrupted while waiting for host to come up");
        } finally {
            cluster.unregister(upListener);
        }
        fail(actual + " did not come up within " + duration + " " + unit);
        return this;
    }

    public HostAssert goesDownWithin(long duration, TimeUnit unit) {
        final CountDownLatch downSignal = new CountDownLatch(1);
        StateListener upListener = new StateListenerBase() {
            public void onDown(Host host) {
                downSignal.countDown();
            }
        };
        cluster.register(upListener);
        try {
            // If the host is already down or if we receive the DOWN signal within given time
            if (actual.state == State.DOWN || downSignal.await(duration, unit))
                return this;
        } catch (InterruptedException e) {
            fail("Got interrupted while waiting for host to go down");
        } finally {
            cluster.unregister(upListener);
        }
        fail(actual + " did not go down within " + duration + " " + unit);
        return this;
    }
}
