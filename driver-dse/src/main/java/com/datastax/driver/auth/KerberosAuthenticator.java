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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetSocketAddress;

/**
 * Responsible for authenticating with secured DSE services using Kerberos
 * over GSSAPI &amp; SASL. The actual SASL negotiation is delegated to a
 * PrivilegedSaslClient which performs the priviledged actions on behalf
 * of the logged in user.
 */
public class KerberosAuthenticator implements Authenticator {
    private static final Logger logger = LoggerFactory.getLogger(KerberosAuthenticator.class);

    public static final String JAAS_CONFIG_ENTRY = "DseClient";
    public static final String[] SUPPORTED_MECHANISMS = new String[]{"GSSAPI"};
    public static final String SASL_PROTOCOL_NAME = "dse";
    public static final String SASL_PROTOCOL_NAME_PROPERTY = "dse.sasl.protocol";

    private final PrivilegedSaslClient saslClient;

    public KerberosAuthenticator(InetSocketAddress host) {
        saslClient = new PrivilegedSaslClient(loginSubject(),
                SUPPORTED_MECHANISMS,
                null,
                System.getProperty(SASL_PROTOCOL_NAME_PROPERTY, SASL_PROTOCOL_NAME),
                host.getAddress().getCanonicalHostName(),
                PrivilegedSaslClient.DEFAULT_PROPERTIES,
                null);
    }

    private Subject loginSubject() {
        Subject subject = new Subject();
        try {
            LoginContext login = new LoginContext(JAAS_CONFIG_ENTRY, subject);
            login.login();
            return subject;
        } catch (LoginException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] initialResponse() {
        return saslClient.getInitialResponse();
    }

    @Override
    public byte[] evaluateChallenge(byte[] challenge) {
        byte[] response = saslClient.evaluateChallenge(challenge);
        if (response == null) {
            // If we generate a null response, then authentication has completed (if
            // not, warn), and return without sending a response back to the server.
            logger.trace("Response to server is null: authentication should now be complete.");
            if (!saslClient.isComplete()) {
                String error = "Client generated a null sasl response, but authentication is not complete.";
                logger.error(error);
                throw new RuntimeException(error);
            }
        }
        return response;
    }

    @Override
    public void onAuthenticationSuccess(byte[] token) {
        // no-op
    }
}
