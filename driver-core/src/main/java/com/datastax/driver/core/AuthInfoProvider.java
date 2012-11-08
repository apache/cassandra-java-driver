package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.*;

/**
 * Authentication informations provider to connect to Cassandra nodes.
 * <p>
 * The authentication information themselves are just a key-value pairs.
 * Which exact key-value pairs are required depends on the authenticator
 * set for the Cassandra nodes.
 */
public interface AuthInfoProvider {

    /**
     * A provider that provides no authencation informations.
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

    /**
     * A simple {@code AuthInfoProvider} implementation.
     * <p>
     * This provider allows to programmatically define authentication
     * information that will then apply to all hosts.
     * <p>
     * Note that it is <b>not</b> safe to add new info to this provider once a
     * Cluster instance has been created using this provider.
     */
    public static class Simple implements AuthInfoProvider {

        private final Map<String, String> credentials = new HashMap<String, String>();

        /**
         * Creates a new, empty, simple authentication info provider.
         */
        public Simple() {}

        /**
         * Creates a new simple authentication info provider with the
         * informations contained in {@code properties}.
         *
         * @param properties a map of authentication information to use.
         */
        public Simple(Map<String, String> properties) {
            this();
            addAll(properties);
        }

        public Map<String, String> getAuthInfos(InetAddress host) {
            return credentials;
        }

        /**
         * Adds a new property to the authentication info returned by this
         * provider.
         *
         * @param property the name of the property to add.
         * @param value the value to add for {@code property}.
         * @return {@code this} object.
         */
        public Simple add(String property, String value) {
            credentials.put(property, value);
            return this;
        }

        /**
         * Adds all the key-value pair provided as new authentication
         * information returned by this provider.
         *
         * @param properties a map of authentication information to add.
         * @return {@code this} object.
         */
        public Simple addAll(Map<String, String> properties) {
            credentials.putAll(properties);
            return this;
        }
    }
}
