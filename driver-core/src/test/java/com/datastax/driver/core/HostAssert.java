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

    protected HostAssert(Host host, Cluster cluster) {
        super(host, HostAssert.class);
        this.cluster = cluster;
    }

    public HostAssert hasState(Host.State expected) {
        assertThat(actual.state).isEqualTo(expected);
        return this;
    }

    public HostAssert isAtDistance(HostDistance expected) {
        LoadBalancingPolicy loadBalancingPolicy = cluster.manager.loadBalancingPolicy();
        assertThat(loadBalancingPolicy.distance(actual)).isEqualTo(expected);
        return this;
    }

    public HostAssert comesUpWithin(long duration, TimeUnit unit) {
        final CountDownLatch upSignal = new CountDownLatch(1);
        StateListener upListener = new StateListenerBase() {
            public void onUp(Host host) {
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

    public static class StateListenerBase implements StateListener {
        @Override
        public void onAdd(Host host) {
        }

        @Override
        public void onUp(Host host) {
        }

        @Override
        public void onSuspected(Host host) {
        }

        @Override
        public void onDown(Host host) {
        }

        @Override
        public void onRemove(Host host) {
        }
    }
}
