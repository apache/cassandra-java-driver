package com.datastax.driver.core;

import com.datastax.driver.core.transport.ConnectionException;

/**
 * The policy with which to decide whether a host should be considered down.
 */
public interface ConvictionPolicy {

    /**
     * Called when a connection error occurs on a connection to the host this policy applies to.
     *
     * @return {@code true} if the host should be considered down.
     */
    public boolean addFailure(ConnectionException exception);

    /**
     * Called when the host has been detected up.
     */
    public void reset();

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
}
