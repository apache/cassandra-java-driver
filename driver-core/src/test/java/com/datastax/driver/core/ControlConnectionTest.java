package com.datastax.driver.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;

public class ControlConnectionTest {
    @Test(groups = "short")
    public void should_prevent_simultaneous_reconnection_attempts() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;

        // Custom load balancing policy that counts the number of calls to newQueryPlan().
        // Since we don't open any session from our Cluster, only the control connection reattempts are calling this
        // method, therefore the invocation count is equal to the number of attempts.
        final AtomicInteger reconnectionAttempts = new AtomicInteger();
        LoadBalancingPolicy loadBalancingPolicy = new QueryPlanCountingPolicy(Policies.defaultLoadBalancingPolicy(),
                                                                              reconnectionAttempts);

        // Custom reconnection policy with a very large delay (longer than the test duration), to make sure we count
        // only the first reconnection attempt of each reconnection handler.
        ReconnectionPolicy reconnectionPolicy = new ReconnectionPolicy() {
            @Override
            public ReconnectionSchedule newSchedule() {
                return new ReconnectionSchedule() {
                    @Override
                    public long nextDelayMs() {
                        return 60 * 1000;
                    }
                };
            }
        };

        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withReconnectionPolicy(reconnectionPolicy)
                             .withLoadBalancingPolicy(loadBalancingPolicy)
                             .build();
            cluster.init();

            ccm.stop(1);

            // Sleep for a while to make sure our final count is not the result of lucky timing
            TimeUnit.SECONDS.sleep(1);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
        assertThat(reconnectionAttempts.get()).isEqualTo(1);
    }

    static class QueryPlanCountingPolicy implements LoadBalancingPolicy {

        private final LoadBalancingPolicy childPolicy;
        private final AtomicInteger counter;

        public QueryPlanCountingPolicy(LoadBalancingPolicy childPolicy, AtomicInteger counter) {
            this.childPolicy = childPolicy;
            this.counter = counter;
        }

        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            counter.incrementAndGet();
            return childPolicy.newQueryPlan(loggedKeyspace, statement);
        }

        public void init(Cluster cluster, Collection<Host> hosts) {
            childPolicy.init(cluster, hosts);
        }

        public HostDistance distance(Host host) {
            return childPolicy.distance(host);
        }

        public void onAdd(Host host) {
            childPolicy.onAdd(host);
        }

        public void onUp(Host host) {
            childPolicy.onUp(host);
        }

        public void onSuspected(Host host) {
            childPolicy.onSuspected(host);
        }

        public void onDown(Host host) {
            childPolicy.onDown(host);
        }

        public void onRemove(Host host) {
            childPolicy.onRemove(host);
        }
    }
}
