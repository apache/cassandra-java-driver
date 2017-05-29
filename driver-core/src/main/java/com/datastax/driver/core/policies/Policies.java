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
package com.datastax.driver.core.policies;

import com.datastax.driver.core.AtomicMonotonicTimestampGenerator;
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
    private static final AddressTranslator DEFAULT_ADDRESS_TRANSLATOR = new IdentityTranslator();
    private static final SpeculativeExecutionPolicy DEFAULT_SPECULATIVE_EXECUTION_POLICY = NoSpeculativeExecutionPolicy.INSTANCE;

    private final LoadBalancingPolicy loadBalancingPolicy;
    private final ReconnectionPolicy reconnectionPolicy;
    private final RetryPolicy retryPolicy;
    private final AddressTranslator addressTranslator;
    private final TimestampGenerator timestampGenerator;
    private final SpeculativeExecutionPolicy speculativeExecutionPolicy;

    private Policies(LoadBalancingPolicy loadBalancingPolicy,
                     ReconnectionPolicy reconnectionPolicy,
                     RetryPolicy retryPolicy,
                     AddressTranslator addressTranslator,
                     TimestampGenerator timestampGenerator,
                     SpeculativeExecutionPolicy speculativeExecutionPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
        this.reconnectionPolicy = reconnectionPolicy;
        this.retryPolicy = retryPolicy;
        this.addressTranslator = addressTranslator;
        this.timestampGenerator = timestampGenerator;
        this.speculativeExecutionPolicy = speculativeExecutionPolicy;
    }

    /**
     * The default load balancing policy.
     * <p/>
     * The default load balancing policy is {@link DCAwareRoundRobinPolicy} with token
     * awareness (so {@code new TokenAwarePolicy(new DCAwareRoundRobinPolicy())}).
     * <p/>
     * Note that this policy shuffles the replicas when token awareness is used, see
     * {@link TokenAwarePolicy#TokenAwarePolicy(LoadBalancingPolicy, boolean)} for an
     * explanation of the tradeoffs.
     *
     * @return the default load balancing policy.
     */
    public static LoadBalancingPolicy defaultLoadBalancingPolicy() {
        // Note: balancing policies are stateful, so we can't store that in a static or that would screw thing
        // up if multiple Cluster instance are started in the same JVM.
        return new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());
    }

    /**
     * The default reconnection policy.
     * <p/>
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
     * <p/>
     * The default retry policy is {@link DefaultRetryPolicy}.
     *
     * @return the default retry policy.
     */
    public static RetryPolicy defaultRetryPolicy() {
        return DEFAULT_RETRY_POLICY;
    }

    /**
     * The default address translator.
     * <p/>
     * The default address translator is {@link IdentityTranslator}.
     *
     * @return the default address translator.
     */
    public static AddressTranslator defaultAddressTranslator() {
        return DEFAULT_ADDRESS_TRANSLATOR;
    }

    /**
     * The default timestamp generator.
     * <p/>
     * This is an instance of {@link AtomicMonotonicTimestampGenerator}.
     *
     * @return the default timestamp generator.
     */
    public static TimestampGenerator defaultTimestampGenerator() {
        return new AtomicMonotonicTimestampGenerator();
    }

    /**
     * The default speculative retry policy.
     * <p/>
     * The default speculative retry policy is a {@link NoSpeculativeExecutionPolicy}.
     *
     * @return the default speculative retry policy.
     */
    public static SpeculativeExecutionPolicy defaultSpeculativeExecutionPolicy() {
        return DEFAULT_SPECULATIVE_EXECUTION_POLICY;
    }

    /**
     * The load balancing policy in use.
     * <p/>
     * The load balancing policy defines how Cassandra hosts are picked for queries.
     *
     * @return the load balancing policy in use.
     */
    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    /**
     * The reconnection policy in use.
     * <p/>
     * The reconnection policy defines how often the driver tries to reconnect to a dead node.
     *
     * @return the reconnection policy in use.
     */
    public ReconnectionPolicy getReconnectionPolicy() {
        return reconnectionPolicy;
    }

    /**
     * The retry policy in use.
     * <p/>
     * The retry policy defines in which conditions a query should be
     * automatically retries by the driver.
     *
     * @return the retry policy in use.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * The address translator in use.
     *
     * @return the address translator in use.
     */
    public AddressTranslator getAddressTranslator() {
        return addressTranslator;
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
        private AddressTranslator addressTranslator;
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
         * @param addressTranslator see {@link #getAddressTranslator()}.
         * @return this builder.
         */
        public Builder withAddressTranslator(AddressTranslator addressTranslator) {
            this.addressTranslator = addressTranslator;
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
         * <p/>
         * Any field that hasn't been set explicitly will get its default value.
         *
         * @return the object.
         */
        public Policies build() {
            return new Policies(
                    loadBalancingPolicy == null ? defaultLoadBalancingPolicy() : loadBalancingPolicy,
                    reconnectionPolicy == null ? defaultReconnectionPolicy() : reconnectionPolicy,
                    retryPolicy == null ? defaultRetryPolicy() : retryPolicy,
                    addressTranslator == null ? defaultAddressTranslator() : addressTranslator,
                    timestampGenerator == null ? defaultTimestampGenerator() : timestampGenerator,
                    speculativeExecutionPolicy == null ? defaultSpeculativeExecutionPolicy() : speculativeExecutionPolicy);
        }
    }
}
