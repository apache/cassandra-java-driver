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

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Authentication information provider to connect to Cassandra nodes.
 * <p>
 * The authentication information consists of key-value pairs.
 * Which exact key-value pairs are required depends on the authenticator
 * set for the Cassandra nodes.
 */
// NOTE: we don't expose that yet, this will change to something better
abstract class AuthInfoProvider {

    /**
     * A provider that provides no authentication information.
     * <p>
     * This is only useful for when no authentication is to be used.
     */
    static final AuthInfoProvider NONE = new AuthInfoProvider() {
        @Override
        public Map<String, String> getAuthInfo(InetAddress host) {
            return Collections.<String, String>emptyMap();
        }
    };

    /**
     * The authentication information to use to connect to {@code host}.
     *
     * Please note that if authentication is required, this method will be
     * called to initialize each new connection created by the driver. It is
     * thus a good idea to make sure this method returns relatively quickly.
     *
     * @param host the Cassandra host for which authentication information
     * is requested.
     * @return The authentication informations to use.
     */
    public abstract Map<String, String> getAuthInfo(InetAddress host);

    static class Simple extends AuthInfoProvider {

        private static final String USERNAME_KEY = "username";
        private static final String PASSWORD_KEY = "password";

        private final Map<String, String> credentials = new HashMap<String, String>(2);

        Simple(String username, String password) {
            credentials.put(USERNAME_KEY, username);
            credentials.put(PASSWORD_KEY, password);
        }

        @Override
        public Map<String, String> getAuthInfo(InetAddress host) {
            return credentials;
        }
    }
}
