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

import com.datastax.driver.core.Authenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.InetAddress;

/**
 * Responsible for authenticating with secured DSE services using Kerberos
 * over GSSAPI & SASL. The actual SASL negotiation is delegated to a
 * PrivilegedSaslClient which performs the priviledged actions on behalf
 * of the logged in user.
 * </p>
 *
 * The SASL protocol name defaults to "dse"; should you need to change that
 * it can be overridden using the <code>dse.sasl.protocol</code> system property.
 * </p>
 *
 * Keytab and ticket cache settings are specified using a standard JAAS
 * configuration file. The location of the file can be set using the
 * <code>java.security.auth.login.config</code> system property or by adding a
 * <code>login.config.url.n</code> entry in the <code>java.security</code> properties
 * file.
 * </p>
 *
 * See {@link http://docs.oracle.com/javase/1.4.2/docs/guide/security/jaas/tutorials/LoginConfigFile.html}
 * for further details on the Login configuration file and
 * {@link http://docs.oracle.com/javase/6/docs/technotes/guides/security/jaas/tutorials/GeneralAcnOnly.html}
 * for more on JAAS in general.
 * </p>
 *
 * <h1>Authentication using ticket cache</h1>
 * Run <code>kinit</code> to obtain a ticket and populate the cache before
 * connecting. JAAS config:

 * <pre>
 * Client {
 *   com.sun.security.auth.module.Krb5LoginModule required
 *     useTicketCache=true
 *     renewTGT=true;
 * };
 * </pre>
 *
 *
 * <h1>Authentication using a keytab file</h1>
 * <p>To enable authentication using a keytab file, specify its location on disk.
 * If your keytab contains more than one principal key, you should also specify
 * which one to select.
 *
 * <pre>
 * Client {
 *     com.sun.security.auth.module.Krb5LoginModule required
 *       useKeyTab=true
 *       keyTab="/path/to/file.keytab"
 *       principal="user@MYDOMAIN.COM";
 * };
 * </pre>
 *
 */
public class KerberosAuthenticator implements Authenticator
{
    private static final Logger logger = LoggerFactory.getLogger(KerberosAuthenticator.class);

    public static final String JAAS_CONFIG_ENTRY = "DseClient";
    public static final String[] SUPPORTED_MECHANISMS = new String[]{"GSSAPI"};
    public static final String SASL_PROTOCOL_NAME = "dse";
    public static final String SASL_PROTOCOL_NAME_PROPERTY = "dse.sasl.protocol";

    private final PrivilegedSaslClient saslClient;

    public KerberosAuthenticator(InetAddress host)
    {
        saslClient = new PrivilegedSaslClient(loginSubject(),
                SUPPORTED_MECHANISMS,
                null,
                System.getProperty(SASL_PROTOCOL_NAME_PROPERTY, SASL_PROTOCOL_NAME),
                host.getCanonicalHostName(),
                PrivilegedSaslClient.DEFAULT_PROPERTIES,
                null);
    }

    private Subject loginSubject()
    {
        Subject subject = new Subject();
        try
        {
            LoginContext login = new LoginContext(JAAS_CONFIG_ENTRY, subject);
            login.login();
            return subject;
        } catch (LoginException e)
        {
            throw new RuntimeException(e);
        }
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
