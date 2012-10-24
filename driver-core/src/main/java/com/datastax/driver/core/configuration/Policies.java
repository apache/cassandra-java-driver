package com.datastax.driver.core.configuration;

public class Policies {

    public static final LoadBalancingPolicy.Factory DEFAULT_LOAD_BALANCING_POLICY_FACTORY = LoadBalancingPolicy.RoundRobin.Factory.INSTANCE;
    public static final ReconnectionPolicy.Factory DEFAULT_RECONNECTION_POLICY_FACTORY = ReconnectionPolicy.Exponential.makeFactory(2 * 1000, 5 * 60 * 1000);
    public static final RetryPolicy DEFAULT_RETRY_POLICY = RetryPolicy.Default.INSTANCE;

    private final LoadBalancingPolicy.Factory loadBalancingPolicyFactory;
    private final ReconnectionPolicy.Factory reconnectionPolicyFactory;
    private final RetryPolicy retryPolicy;

    public Policies(LoadBalancingPolicy.Factory loadBalancingPolicyFactory,
                    ReconnectionPolicy.Factory reconnectionPolicyFactory,
                    RetryPolicy retryPolicy) {

        this.loadBalancingPolicyFactory = loadBalancingPolicyFactory;
        this.reconnectionPolicyFactory = reconnectionPolicyFactory;
        this.retryPolicy = retryPolicy;
    }

    public LoadBalancingPolicy.Factory getLoadBalancingPolicyFactory() {
        return loadBalancingPolicyFactory;
    }

    public ReconnectionPolicy.Factory getReconnectionPolicyFactory() {
        return reconnectionPolicyFactory;
    }

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }
}
