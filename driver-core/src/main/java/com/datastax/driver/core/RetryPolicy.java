package com.datastax.driver.core;

/**
 * A policy that defines a default comportment to adopt when a request returns
 * a TimeoutException or an UnavailableException.
 *
 * TODO: is that really useful to have such details if one cannot modify the request?
 */
public interface RetryPolicy {

    public boolean onReadTimeout(ConsistencyLevel cl, int required, int received, boolean dataPresent, int nbRetry);

    public boolean onWriteTimeout(ConsistencyLevel cl, int required, int received, int nbRetry);

    public boolean onUnavailable(ConsistencyLevel cl, int required, int alive, int nbRetry);

    public static class DefaultPolicy implements RetryPolicy {

        public static final DefaultPolicy INSTANCE = new DefaultPolicy();

        private DefaultPolicy() {}

        public boolean onReadTimeout(ConsistencyLevel cl, int required, int received, boolean dataPresent, int nbRetry) {
            if (nbRetry > 1)
                return false;

            return received >= required && !dataPresent;
        }

        public boolean onWriteTimeout(ConsistencyLevel cl, int required, int received, int nbRetry) {
            return false;
        }

        public boolean onUnavailable(ConsistencyLevel cl, int required, int alive, int nbRetry) {
            return false;
        }
    }
}
