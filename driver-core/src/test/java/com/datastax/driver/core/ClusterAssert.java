package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;
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
        Host host = findHost(actual, hostNumber);

        return new HostAssert(host, actual);
    }

    /** Utility method to find the {@code Host} object corresponding to a node in a cluster. */
    public static Host findHost(Cluster cluster, int hostNumber) {
        String address = CCMBridge.ipOfNode(hostNumber);
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (host.getAddress().getHostAddress().equals(address))
                return host;
        }
        fail(address + " not found in cluster metadata");
        return null; // never reached
    }
}
