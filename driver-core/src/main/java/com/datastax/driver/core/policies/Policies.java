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
package com.datastax.driver.core.policies;

import com.google.common.base.Objects;

import com.datastax.driver.core.ServerSideTimestampGenerator;
import com.datastax.driver.core.TimestampGenerator;

/**
 * Policies configured for a {@link com.datastax.driver.core.Cluster} instance.
 */
public class Policies {

    /**
     * Returns a builder to create a new {@code Policies} object.
     *
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private static final ReconnectionPolicy DEFAULT_RECONNECTION_POLICY = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);
    private static final RetryPolicy DEFAULT_RETRY_POLICY = DefaultRetryPolicy.INSTANCE;
    private static final AddressTranslater DEFAULT_ADDRESS_TRANSLATER = new IdentityTranslater();
    private static final SpeculativeExecutionPolicy DEFAULT_SPECULATIVE_EXECUTION_POLICY = NoSpeculativeExecutionPolicy.INSTANCE;

    private final LoadBalancingPolicy loadBalancingPolicy;
    private final ReconnectionPolicy reconnectionPolicy;
    private final RetryPolicy retryPolicy;
    private final AddressTranslater addressTranslater;
    private final TimestampGenerator timestampGenerator;
    private final SpeculativeExecutionPolicy speculativeExecutionPolicy;

    /**
     * @deprecated this constructor is exposed for backward compatibility.
     * Use {@link #builder()} instead.
     */
    @Deprecated
    /*
     * This constructor should be private and called only by the builder.
     * If a new field gets added, add it to this constructor and make it private, then expose the current signature as a deprecated method
     * (see end of class).
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy,
                    ReconnectionPolicy reconnectionPolicy,
                    RetryPolicy retryPolicy,
                    AddressTranslater addressTranslater,
                    TimestampGenerator timestampGenerator,
                    SpeculativeExecutionPolicy speculativeExecutionPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
        this.reconnectionPolicy = reconnectionPolicy;
        this.retryPolicy = retryPolicy;
        this.addressTranslater = addressTranslater;
        this.timestampGenerator = timestampGenerator;
        this.speculativeExecutionPolicy = speculativeExecutionPolicy;
    }

    /**
     * The default load balancing policy.
     * <p>
     * The default load balancing policy is {@link DCAwareRoundRobinPolicy} with token
     * awareness (so {@code new TokenAwarePolicy(new DCAwareRoundRobinPolicy())}).
     * <p>
     * Note that this policy shuffles the replicas when token awareness is used, see
     * {@link TokenAwarePolicy#TokenAwarePolicy(LoadBalancingPolicy, boolean)} for an
     * explanation of the tradeoffs.
     *
     * @return the default load balancing policy.
     */
    public static LoadBalancingPolicy defaultLoadBalancingPolicy() {
        // Note: balancing policies are stateful, so we can't store that in a static or that would screw thing
        // up if multiple Cluster instance are started in the same JVM.
        return new TokenAwarePolicy(new DCAwareRoundRobinPolicy());
    }

    /**
     * The default reconnection policy.
     * <p>
     * The default reconnection policy is an {@link ExponentialReconnectionPolicy}
     * where the base delay is 1 second and the max delay is 10 minutes;
     *
     * @return the default reconnection policy.
     */
    public static ReconnectionPolicy defaultReconnectionPolicy() {
        return DEFAULT_RECONNECTION_POLICY;
    }

    /**
     * The default retry policy.
     * <p>
     * The default retry policy is {@link DefaultRetryPolicy}.
     *
     * @return the default retry policy.
     */
    public static RetryPolicy defaultRetryPolicy() {
        return DEFAULT_RETRY_POLICY;
    }

    /**
     * The default address translater.
     * <p>
     * The default address translater is {@link IdentityTranslater}.
     *
     * @return the default address translater.
     */
    public static AddressTranslater defaultAddressTranslater() {
        return DEFAULT_ADDRESS_TRANSLATER;
    }

    /**
     * The default timestamp generator.
     * <p>
     * This is an instance of {@link ServerSideTimestampGenerator}.
     *
     * @return the default timestamp generator.
     */
    public static TimestampGenerator defaultTimestampGenerator() {
        return ServerSideTimestampGenerator.INSTANCE;
    }

    /**
     * The default speculative retry policy.
     * <p>
     * The default speculative retry policy is a {@link NoSpeculativeExecutionPolicy}.
     *
     * @return the default speculative retry policy.
     */
    public static SpeculativeExecutionPolicy defaultSpeculativeExecutionPolicy() {
        return DEFAULT_SPECULATIVE_EXECUTION_POLICY;
    }

    /**
     * The load balancing policy in use.
     * <p>
     * The load balancing policy defines how Cassandra hosts are picked for queries.
     *
     * @return the load balancing policy in use.
     */
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    /**
     * The reconnection policy in use.
     * <p>
     * The reconnection policy defines how often the driver tries to reconnect to a dead node.
     *
     * @return the reconnection policy in use.
     */
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    /**
     * The retry policy in use.
     * <p>
     * The retry policy defines in which conditions a query should be
     * automatically retries by the driver.
     *
     * @return the retry policy in use.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * The address translater in use.
     *
     * @return the address translater in use.
     */
    public AddressTranslater getAddressTranslater() {
        return addressTranslater;
    }

    /**
     * The timestamp generator to use.
     *
     * @return the timestamp generator to use.
     */
    public TimestampGenerator getTimestampGenerator() {
        return timestampGenerator;
    }

    /**
     * The speculative execution policy in use.
     *
     * @return the speculative execution policy in use.
     */
    public SpeculativeExecutionPolicy getSpeculativeExecutionPolicy() {
        return speculativeExecutionPolicy;
    }

    /**
     * A builder to create a new {@code Policies} object.
     */
    public static class Builder {
        private LoadBalancingPolicy loadBalancingPolicy;
        private ReconnectionPolicy reconnectionPolicy;
        private RetryPolicy retryPolicy;
        private AddressTranslater addressTranslater;
        private TimestampGenerator timestampGenerator;
        private SpeculativeExecutionPolicy speculativeExecutionPolicy;

        /**
         * Sets the load balancing policy.
         *
         * @param loadBalancingPolicy see {@link #getLoadBalancingPolicy()}.
         * @return this builder.
         */
        public Builder withLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
            this.loadBalancingPolicy = loadBalancingPolicy;
            return this;
        }

        /**
         * Sets the reconnection policy.
         *
         * @param reconnectionPolicy see {@link #getReconnectionPolicy()}.
         * @return this builder.
         */
        public Builder withReconnectionPolicy(ReconnectionPolicy reconnectionPolicy) {
            this.reconnectionPolicy = reconnectionPolicy;
            return this;
        }

        /**
         * Sets the retry policy.
         *
         * @param retryPolicy see {@link #getRetryPolicy()}.
         * @return this builder.
         */
        public Builder withRetryPolicy(RetryPolicy retryPolicy) {
            this.retryPolicy = retryPolicy;
            return this;
        }

        /**
         * Sets the address translator.
         *
         * @param addressTranslater see {@link #getAddressTranslater()}.
         * @return this builder.
         */
        public Builder withAddressTranslater(AddressTranslater addressTranslater) {
            this.addressTranslater = addressTranslater;
            return this;
        }

        /**
         * Sets the timestamp generator.
         *
         * @param timestampGenerator see {@link #getTimestampGenerator()}.
         * @return this builder.
         */
        public Builder withTimestampGenerator(TimestampGenerator timestampGenerator) {
            this.timestampGenerator = timestampGenerator;
            return this;
        }

        /**
         * Sets the speculative execution policy.
         *
         * @param speculativeExecutionPolicy see {@link #getSpeculativeExecutionPolicy()}.
         * @return this builder.
         */
        public Builder withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy speculativeExecutionPolicy) {
            this.speculativeExecutionPolicy = speculativeExecutionPolicy;
            return this;
        }

        /**
         * Builds the final object from this builder.
         * <p>
         * Any field that hasn't been set explicitly will get its default value.
         *
         * @return the object.
         */
        public Policies build() {
            return new Policies(
                loadBalancingPolicy == null ? Policies.defaultLoadBalancingPolicy() : loadBalancingPolicy,
                Objects.firstNonNull(reconnectionPolicy, Policies.defaultReconnectionPolicy()),
                Objects.firstNonNull(retryPolicy, Policies.defaultRetryPolicy()),
                Objects.firstNonNull(addressTranslater, Policies.defaultAddressTranslater()),
                Objects.firstNonNull(timestampGenerator, Policies.defaultTimestampGenerator()),
                Objects.firstNonNull(speculativeExecutionPolicy, Policies.defaultSpeculativeExecutionPolicy()));
        }
    }

    /*
     * The public constructors of this class were mistakenly changed over time (this is a breaking change).
     *
     * The code below provides backward compatibility with every signature that was ever exposed in a released version.
     * This is ugly but the safest way to avoid issues for clients upgrading from a previous version.
     *
     * In the future, new fields will only be added to the builder, which avoids this problem.
     */

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    @Deprecated
    public Policies() {
        this(defaultLoadBalancingPolicy(), defaultReconnectionPolicy(), defaultRetryPolicy(), defaultAddressTranslater(), defaultTimestampGenerator(), defaultSpeculativeExecutionPolicy());
    }

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy, ReconnectionPolicy reconnectionPolicy, RetryPolicy retryPolicy) {
        this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, defaultAddressTranslater(), defaultTimestampGenerator(), defaultSpeculativeExecutionPolicy());
    }

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy, ReconnectionPolicy reconnectionPolicy, RetryPolicy retryPolicy, AddressTranslater addressTranslater) {
        this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, addressTranslater, defaultTimestampGenerator(), defaultSpeculativeExecutionPolicy());
    }

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy, ReconnectionPolicy reconnectionPolicy, RetryPolicy retryPolicy, SpeculativeExecutionPolicy speculativeExecutionPolicy) {
        this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, defaultAddressTranslater(), defaultTimestampGenerator(), speculativeExecutionPolicy);
    }

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy, ReconnectionPolicy reconnectionPolicy, RetryPolicy retryPolicy, AddressTranslater addressTranslater, TimestampGenerator timestampGenerator) {
        this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, addressTranslater, timestampGenerator, defaultSpeculativeExecutionPolicy());
    }

    /**
     * @deprecated this constructor is provided for backward compatibility.
     * Use {@link #builder()} instead.
     */
    public Policies(LoadBalancingPolicy loadBalancingPolicy, ReconnectionPolicy reconnectionPolicy, RetryPolicy retryPolicy, AddressTranslater addressTranslater, SpeculativeExecutionPolicy speculativeExecutionPolicy) {
        this(loadBalancingPolicy, reconnectionPolicy, retryPolicy, addressTranslater, defaultTimestampGenerator(), speculativeExecutionPolicy);
    }
}
