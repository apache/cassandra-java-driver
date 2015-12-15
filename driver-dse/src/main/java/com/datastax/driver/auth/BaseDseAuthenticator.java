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
package com.datastax.driver.auth;

import com.datastax.driver.core.Authenticator;

/**
 * Base class for {@link Authenticator} implementations that want to make use of
 * the authentication scheme negotiation in the DseAuthenticator
 */
abstract class BaseDseAuthenticator implements Authenticator {
    private static final String DSE_AUTHENTICATOR = "com.datastax.bdp.cassandra.auth.DseAuthenticator";
    private final String authenticator;

    protected BaseDseAuthenticator(String authenticator) {
        this.authenticator = authenticator;
    }

    /**
     * Return a byte array containing the required SASL mechanism.
     * This should be one of:
     * <ul>
     * <li>"PLAIN".getBytes(Charsets.UTF_8);</li>
     * <li>"GSSAPI".getBytes(Charsets.UTF_8);</li>
     * </ul>
     *
     * @return a byte array containing the SASL mechanism
     */
    public abstract byte[] getMechanism();

    /**
     * Return a byte array containing the expected successful server challenge.
     * This should be one of:
     * <ul>
     * <li>"PLAIN-START".getBytes(Charsets.UTF_8);</li>
     * <li>"GSSAPI-START".getBytes(Charsets.UTF_8);</li>
     * </ul>
     *
     * @return a byte array containing the server challenge
     */
    public abstract byte[] getInitialServerChallenge();

    public byte[] initialResponse() {
        if (isDseAuthenticator())
            return getMechanism();
        else
            return evaluateChallenge(getInitialServerChallenge());
    }

    public void onAuthenticationSuccess(byte[] token) {
    }

    private boolean isDseAuthenticator() {
        return authenticator.equals(DSE_AUTHENTICATOR);
    }
}
