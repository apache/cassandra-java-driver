/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

/**
 * Handles SASL authentication with Cassandra servers.
 * <p/>
 * Each time a new connection is created and the server requires authentication,
 * a new instance of this class will be created by the corresponding
 * {@link AuthProvider} to handle that authentication. The lifecycle of that
 * new {@code Authenticator} will be:
 * <ol>
 * <li>The {@code initialResponse} method will be called. The initial return
 * value will be sent to the server to initiate the handshake.</li>
 * <li>The server will respond to each client response by either issuing a
 * challenge or indicating that the authentication is complete (successfully or not).
 * If a new challenge is issued, the authenticator {@code evaluateChallenge}
 * method will be called to produce a response that will be sent to the
 * server. This challenge/response negotiation will continue until the server
 * responds that authentication is successful (or an {@code AuthenticationException}
 * is raised).
 * </li>
 * <li>When the server indicates that authentication is successful, the
 * {@code onAuthenticationSuccess} method will be called with the last information
 * that the server may optionally have sent.
 * </li>
 * </ol>
 * The exact nature of the negotiation between client and server is specific
 * to the authentication mechanism configured server side.
 */
public interface Authenticator {

    /**
     * Obtain an initial response token for initializing the SASL handshake
     *
     * @return the initial response to send to the server, may be null
     */
    public byte[] initialResponse();

    /**
     * Evaluate a challenge received from the Server. Generally, this method
     * should return null when authentication is complete from the client
     * perspective
     *
     * @param challenge the server's SASL challenge
     * @return updated SASL token, may be null to indicate the client
     * requires no further action
     */
    public byte[] evaluateChallenge(byte[] challenge);

    /**
     * Called when authentication is successful with the last information
     * optionally sent by the server.
     *
     * @param token the information sent by the server with the authentication
     *              successful message. This will be {@code null} if the server sends no
     *              particular information on authentication success.
     */
    public void onAuthenticationSuccess(byte[] token);
}
