package com.datastax.driver.core;

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
        final AtomicBoolean notification = new AtomicBoolean();
        StateListener upListener = new StateListenerBase() {
            public void onUp(Host host) {
                notification.set(true);
            };
        };
        cluster.register(upListener);
        try {
            // Make sure the host did not come up while we were installing the listener
            if (actual.isUp())
                return this;

            long maxTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(duration, unit);
            do {
                TimeUnit.SECONDS.sleep(10);
                if (notification.get())
                    return this;
            } while (System.nanoTime() < maxTime);
        } catch (InterruptedException e) {
            fail("Got interrupted while waiting for host to come up");
        } finally {
            cluster.unregister(upListener);
        }
        fail(actual + " did not come up within " + duration + " " + unit);
        return this;
    }

    public HostAssert goesDownWithin(long duration, TimeUnit unit) {
        final AtomicBoolean notification = new AtomicBoolean();
        StateListener upListener = new StateListenerBase() {
            public void onDown(Host host) {
                notification.set(true);
            };
        };
        cluster.register(upListener);
        try {
            // Make sure the host did not go down while we were installing the listener
            if (actual.state == State.DOWN)
                return this;

            long maxTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(duration, unit);
            do {
                TimeUnit.SECONDS.sleep(10);
                if (notification.get())
                    return this;
            } while (System.nanoTime() < maxTime);
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
