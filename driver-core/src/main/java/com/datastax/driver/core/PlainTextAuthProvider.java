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

import javax.security.sasl.SaslException;
import java.net.InetAddress;
import java.util.Map;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

/**
 * A simple {@code AuthProvider} implementation.
 * <p>
 * This provider allows to programmatically define authentication
 * information that will then apply to all hosts. The
 * PlainTextAuthenticator instances it returns support SASL
 * authentication using the PLAIN mechanism for version 2 of the
 * CQL native protocol.
 */
public class PlainTextAuthProvider implements AuthProvider {

    private final String username;
    private final String password;

    /**
     * Creates a new simple authentication information provider with the
     * supplied credentials.
     * @param username to use for authentication requests
     * @param password to use for authentication requests
     */
    public PlainTextAuthProvider(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Uses the supplied credentials and the SASL PLAIN mechanism to login
     * to the server.
     *
     * @param host the Cassandra host with which we want to authenticate
     * @return an Authenticator instance which can be used to perform
     * authentication negotiations on behalf of the client
     * @throws SaslException if an unsupported SASL mechanism is supplied
     * or an error is encountered when initialising the authenticator
     */
    public Authenticator newAuthenticator(InetAddress host) {
        return new PlainTextAuthenticator(username, password);
    }

    /**
     * Simple implementation of {@link Authenticator} which can
     * perform authentication against Cassandra servers configured
     * with PasswordAuthenticator.
     */
    private static class PlainTextAuthenticator extends ProtocolV1Authenticator implements Authenticator {

        private final byte[] username;
        private final byte[] password;

        public PlainTextAuthenticator(String username, String password) {
            this.username = username.getBytes(Charsets.UTF_8);
            this.password = password.getBytes(Charsets.UTF_8);
        }

        @Override
        public byte[] initialResponse() {
            byte[] initialToken = new byte[username.length + password.length + 2];
            initialToken[0] = 0;
            System.arraycopy(username, 0, initialToken, 1, username.length);
            initialToken[username.length + 1] = 0;
            System.arraycopy(password, 0, initialToken, username.length + 2, password.length);
            return initialToken;
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge) {
            return null;
        }

        @Override
        public void onAuthenticationSuccess(byte[] token) {
            // no-op, the server should send nothing anyway
        }

        Map<String, String> getCredentials() {
            return ImmutableMap.of("username", new String(username, Charsets.UTF_8),
                                   "password", new String(password, Charsets.UTF_8));
        }
    }
}
