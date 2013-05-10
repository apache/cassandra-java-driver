/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.sasl;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.exceptions.AuthenticationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import java.io.IOException;
import java.net.InetAddress;

/**
 * Additional authenticator to verify that the SASL client implementation works with
 * mechanism other than GSSAPI. This authenticator uses the full SASL PLAIN mechanism
 * and may be used with Cassandra's PasswordAuthenticator
 */
public class PlainTextSaslAuthProvider implements AuthProvider
{
    private static final Logger logger = LoggerFactory.getLogger(PlainTextSaslAuthProvider.class);

    private final String username;
    private final String password;

    public PlainTextSaslAuthProvider(String username, String password)
    {
        this.username = username;
        this.password = password;
    }

    @Override
    public Authenticator newAuthenticator(InetAddress host) throws AuthenticationException
    {
        return new PlainTextSaslAuthenticator(host, username, password);
    }

    private static class PlainTextSaslAuthenticator implements Authenticator
    {

        public static final String[] SUPPORTED_MECHANISMS = new String[]{"PLAIN"};
        public static final String PROTOCOL_NAME = "cassandra";

        private final PrivilegedSaslClient saslClient;

        public PlainTextSaslAuthenticator(InetAddress host, String username, String password)
        {
            saslClient = new PrivilegedSaslClient(new Subject(),
                    SUPPORTED_MECHANISMS,
                    null,
                    PROTOCOL_NAME,
                    host.getCanonicalHostName(),
                    PrivilegedSaslClient.DEFAULT_PROPERTIES,
                    getCallbackHandler(username, password));
        }

        private CallbackHandler getCallbackHandler(final String username, final String password)
        {
            return new CallbackHandler()
            {
                @Override
                public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
                {
                    for (Callback cb : callbacks)
                    {
                        if (cb instanceof NameCallback)
                            ((NameCallback) cb).setName(username);
                        else if (cb instanceof PasswordCallback)
                            ((PasswordCallback) cb).setPassword(password.toCharArray());
                    }
                }
            };
        }

        @Override
        public byte[] initialResponse()
        {
            return saslClient.getInitialResponse();
        }

        @Override
        public byte[] evaluateChallenge(byte[] challenge)
        {
            byte[] response = saslClient.evaluateChallenge(challenge);
            if (response == null)
            {
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
    }
}
