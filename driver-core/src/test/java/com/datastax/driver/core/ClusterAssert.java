package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import org.assertj.core.api.AbstractAssert;

public class ClusterAssert extends AbstractAssert<ClusterAssert, Cluster> {
    protected ClusterAssert(Cluster actual) {
        super(actual, ClusterAssert.class);
    }

    public ClusterAssert usesControlHost(int node) {
        String expectedAddress = CCMBridge.ipOfNode(node);
        Host controlHost = actual.manager.controlConnection.connectedHost();
        assertThat(controlHost.getAddress().getHostAddress()).isEqualTo(expectedAddress);
        return this;
    }

    public HostAssert host(int hostNumber) {
        // TODO at some point this won't work anymore if we have assertions that wait for a node to
        // join the cluster, e.g. assertThat(cluster).node(3).comesUp().
        Host host = TestUtils.findHost(actual, hostNumber);

        return new HostAssert(host, actual);
    }
}
