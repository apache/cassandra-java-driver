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

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.Host.StateListener;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import org.assertj.core.api.AbstractAssert;

import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.ConditionChecker.check;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
        // Ensure that host is not attempting a reconnect.  Because of JAVA-970 we cannot
        // be sure that there is a race and another pool is created before the host is marked down so we
        // check to see it stops after 30 seconds.
        // TODO: Change this to check only once if JAVA-970 is fixed.
        check().before(30, TimeUnit.SECONDS).that(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                // Whether or not host is down and reconnection attempt is in progress.
                return actual.getReconnectionAttemptFuture() != null && !actual.getReconnectionAttemptFuture().isDone();
            }
        }).becomesFalse();
        return this.isDown();
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
            @Override
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

    @SuppressWarnings("deprecation")
    public HostAssert hasWorkload(String workload) {
        assertThat(actual.getDseWorkload()).isNotNull().isEqualTo(workload);
        return this;
    }

    @SuppressWarnings("deprecation")
    public HostAssert hasNoWorkload() {
        assertThat(actual.getDseWorkload()).isNull();
        return this;
    }

    @SuppressWarnings("deprecation")
    public HostAssert hasDseVersion(VersionNumber versionNumber) {
        assertThat(actual.getDseVersion()).isNotNull().isEqualTo(versionNumber);
        return this;
    }

    @SuppressWarnings("deprecation")
    public HostAssert hasNoDseVersion() {
        assertThat(actual.getDseVersion()).isNull();
        return this;
    }

    @SuppressWarnings("deprecation")
    public HostAssert hasDseGraph() {
        assertThat(actual.isDseGraphEnabled()).isTrue();
        return this;
    }

    @SuppressWarnings("deprecation")
    public HostAssert hasNoDseGraph() {
        assertThat(actual.isDseGraphEnabled()).isFalse();
        return this;
    }

    public HostAssert hasListenAddress(InetAddress address) {
        assertThat(actual.getListenAddress()).isNotNull().isEqualTo(address);
        return this;
    }

    public HostAssert hasNoListenAddress() {
        assertThat(actual.getListenAddress()).isNull();
        return this;
    }

    public HostAssert hasBroadcastAddress(InetAddress address) {
        assertThat(actual.getBroadcastAddress()).isNotNull().isEqualTo(address);
        return this;
    }

    public HostAssert hasNoBroadcastAddress() {
        assertThat(actual.getBroadcastAddress()).isNull();
        return this;
    }
}
