package com.datastax.driver.core;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Iterator;

import org.testng.annotations.*;

import static org.testng.Assert.assertEquals;

import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

public class LoadBalancingPolicyBootstrapTest {
    CountingPolicy policy;
    CCMBridge.CCMCluster c;

    @BeforeClass(groups = "short")
    private void setup() {
        policy = new CountingPolicy(new RoundRobinPolicy());
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(policy);
        c = CCMBridge.buildCluster(2, builder);
    }

    @Test(groups = "short")
    public void notificationsTest() throws Exception {
        assertEquals(policy.adds, 2, "adds\n" + policy.history);
        assertEquals(policy.suspecteds, 0, "suspecteds\n" + policy.history);
        assertEquals(policy.removes, 0, "removes\n" + policy.history);
        assertEquals(policy.ups, 0, "ups\n" + policy.history);
        assertEquals(policy.downs, 0, "downs\n" + policy.history);
    }

    @AfterClass(groups = "short")
    private void tearDown() {
        c.discard();
    }

    static class CountingPolicy implements LoadBalancingPolicy {

        private final LoadBalancingPolicy delegate;
        int adds;
        int suspecteds;
        int removes;
        int ups;
        int downs;

        final StringWriter history = new StringWriter();
        private final PrintWriter out = new PrintWriter(history);

        CountingPolicy(LoadBalancingPolicy delegate) {
            this.delegate = delegate;
        }

        public void onAdd(Host host) {
            out.printf("add %s%n", host);
            adds++;
            delegate.onAdd(host);
        }

        public void onSuspected(Host host) {
            out.printf("suspect %s%n", host);
            suspecteds++;
            delegate.onSuspected(host);
        }

        public void onUp(Host host) {
            out.printf("up %s%n", host);
            ups++;
            delegate.onUp(host);
        }

        public void onDown(Host host) {
            out.printf("down %s%n", host);
            downs++;
            delegate.onDown(host);
        }

        public void onRemove(Host host) {
            out.printf("remove %s%n", host);
            removes++;
            delegate.onRemove(host);
        }

        public void init(Cluster cluster, Collection<Host> hosts) {
            delegate.init(cluster, hosts);
        }

        public HostDistance distance(Host host) {
            return delegate.distance(host);
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            return delegate.newQueryPlan(loggedKeyspace, statement);
        }
    }
}
