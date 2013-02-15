/*
 *      Copyright (C) 2012 DataStax Inc.
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
package com.datastax.driver.core;

/**
 * The policy with which to decide whether a host should be considered down.
 *
 * TODO: this class is fully abstract (rather than an interface) because I'm
 * not sure it's worth exposing (and if we do expose it, we need to expose
 * ConnectionException). Maybe just exposing say a threshold of error before
 * convicting a node is enough.
 */
abstract class ConvictionPolicy {

    /**
     * Called when a connection error occurs on a connection to the host this policy applies to.
     *
     * @param exception the connection error that occurred.
     *
     * @return {@code true} if the host should be considered down.
     */
    public abstract boolean addFailure(ConnectionException exception);

    /**
     * Called when the host has been detected up.
     */
    public abstract void reset();

    /**
     * Simple factory interface to allow creating {@link ConvictionPolicy} instances.
     */
    public interface Factory {

        /**
         * Creates a new ConvictionPolicy instance for {@code host}.
         *
         * @param host the host this policy applies to
         * @return the newly created {@link ConvictionPolicy} instance.
         */
        public ConvictionPolicy create(Host host);
    }

    public static class Simple extends ConvictionPolicy {

        private final Host host;

        private Simple(Host host) {
            this.host = host;
        }

        public boolean addFailure(ConnectionException exception) {
            return true;
        }

        public boolean addFailureFromExternalDetector() {
            return true;
        }

        public void reset() {}

        public static class Factory implements ConvictionPolicy.Factory {

            public ConvictionPolicy create(Host host) {
                return new Simple(host);
            }
        }
    }
}
