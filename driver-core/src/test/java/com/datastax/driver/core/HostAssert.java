package com.datastax.driver.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.LoadBalancingPolicy;

import org.assertj.core.api.AbstractAssert;

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
}
