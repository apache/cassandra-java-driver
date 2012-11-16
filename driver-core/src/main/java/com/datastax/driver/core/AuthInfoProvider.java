package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;

/**
 * Authentication informations provider to connect to Cassandra nodes.
 * <p>
 * The authentication information themselves are just a key-value pairs.
 * Which exact key-value pairs are required depends on the authenticator
 * set for the Cassandra nodes.
 */
public interface AuthInfoProvider {

    /**
     * A provider that provides no authentication informations.
     * <p>
     * This is only useful for when no authentication is to be used.
     */
    public static final AuthInfoProvider NONE = new AuthInfoProvider() {
        public Map<String, String> getAuthInfos(InetAddress host) {
            return Collections.<String, String>emptyMap();
        }
    };

    /**
     * The authentication informations to use to connect to {@code host}.
     *
     * Please note that if authentication is required, this method will be
     * called to initialize each new connection created by the driver. It is
     * thus a good idea to make sure this method returns relatively quickly.
     *
     * @param host the Cassandra host for which authentication information
     * are requested.
     * @return The authentication informations to use.
     */
    public Map<String, String> getAuthInfos(InetAddress host);
}
