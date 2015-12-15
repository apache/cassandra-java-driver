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

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.net.InetSocketAddress;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;

/**
 * AuthProvider that provides GSSAPI authenticator instances for clients to connect
 * to DSE clusters secured with the DseAuthenticator.
 * <p/>
 * <h1>Kerberos Authentication</h1>
 * The SASL protocol name defaults to "dse"; should you need to change that
 * it can be overridden using the <code>dse.sasl.protocol</code> system property.
 * <p/>
 * Keytab and ticket cache settings are specified using a standard JAAS
 * configuration file. The location of the file can be set using the
 * <code>java.security.auth.login.config</code> system property or by adding a
 * <code>login.config.url.n</code> entry in the <code>java.security</code> properties
 * file.
 * <p/>
 * See the following documents for further details on the
 * <a href="https://docs.oracle.com/javase/6/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html">JAAS Login Configuration File</a> and the
 * <a href="http://docs.oracle.com/javase/6/docs/technotes/guides/security/jaas/tutorials/GeneralAcnOnly.html">JAAS Authentication Tutorial</a>
 * for more on JAAS in general.
 * <p/>
 * <h1>Authentication using ticket cache</h1>
 * Run <code>kinit</code> to obtain a ticket and populate the cache before
 * connecting. JAAS config:
 * <p/>
 * <pre>
 * DseClient {
 *   com.sun.security.auth.module.Krb5LoginModule required
 *     useTicketCache=true
 *     renewTGT=true;
 * };
 * </pre>
 * <p/>
 * <p/>
 * <h1>Authentication using a keytab file</h1>
 * To enable authentication using a keytab file, specify its location on disk.
 * If your keytab contains more than one principal key, you should also specify
 * which one to select.
 * <p/>
 * <pre>
 * DseClient {
 *     com.sun.security.auth.module.Krb5LoginModule required
 *       useKeyTab=true
 *       keyTab="/path/to/file.keytab"
 *       principal="user@MYDOMAIN.COM";
 * };
 * </pre>
 * To create a cluster using this auth provider:
 * <pre>
 * Cluster cluster = Cluster.builder()
 *                          .addContactPoint(hostname)
 *                          .withAuthProvider(new DseGSSAPIAuthProvider())
 *                          .build();
 * </pre>
 */
public class DseGSSAPIAuthProvider implements AuthProvider {
    public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException {
        return new GSSAPIAuthenticator(authenticator, host);
    }

    private static class GSSAPIAuthenticator extends BaseDseAuthenticator {
        private static final String JAAS_CONFIG_ENTRY = "DseClient";
        private static final String[] SUPPORTED_MECHANISMS = new String[]{"GSSAPI"};
        private static final String SASL_PROTOCOL_NAME = "dse";
        private static final String SASL_PROTOCOL_NAME_PROPERTY = "dse.sasl.protocol";
        private static final Map<String, String> DEFAULT_PROPERTIES =
                ImmutableMap.<String, String>builder().put(Sasl.SERVER_AUTH, "true")
                        .put(Sasl.QOP, "auth")
                        .build();
        private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
        private static final byte[] MECHANISM = "GSSAPI".getBytes(Charsets.UTF_8);
        private static final byte[] SERVER_INITIAL_CHALLENGE = "GSSAPI-START".getBytes(Charsets.UTF_8);

        private final Subject subject;
        private final SaslClient saslClient;

        public GSSAPIAuthenticator(String authenticator, InetSocketAddress host) {
            super(authenticator);
            try {
                LoginContext login = new LoginContext(JAAS_CONFIG_ENTRY);
                login.login();
                subject = login.getSubject();
                saslClient = Sasl.createSaslClient(SUPPORTED_MECHANISMS,
                        null,
                        System.getProperty(SASL_PROTOCOL_NAME_PROPERTY, SASL_PROTOCOL_NAME),
                        host.getAddress().getCanonicalHostName(),
                        DEFAULT_PROPERTIES,
                        null);
            } catch (LoginException e) {
                throw new RuntimeException(e);
            } catch (SaslException e) {
                throw new RuntimeException(e);
            }
        }

        public byte[] getMechanism() {
            return MECHANISM.clone();
        }

        public byte[] getInitialServerChallenge() {
            return SERVER_INITIAL_CHALLENGE.clone();
        }

        public byte[] evaluateChallenge(byte[] challenge) {
            if (Arrays.equals(SERVER_INITIAL_CHALLENGE, challenge)) {
                if (!saslClient.hasInitialResponse()) {
                    return EMPTY_BYTE_ARRAY;
                }
                challenge = EMPTY_BYTE_ARRAY;
            }
            final byte[] internalChallenge = challenge;
            try {
                return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run() throws SaslException {
                        return saslClient.evaluateChallenge(internalChallenge);
                    }
                });
            } catch (PrivilegedActionException e) {
                throw new RuntimeException(e.getException());
            }
        }
    }
}
