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
