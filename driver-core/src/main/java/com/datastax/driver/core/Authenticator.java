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
package com.datastax.driver.core;

/**
 * Handles SASL authentication with Cassandra servers.
 *
 * A server which requires authentication responds to a startup
 * message with an challenge in the form of an {@code AuthenticateMessage}.
 * Authenticator implementations should be able to respond to that
 * challenge and perform whatever authentication negotiation is required
 * by the server. The exact nature of that negotiation is specific to the
 * configuration of the server.
 */
public interface Authenticator {

    /**
     * Obtain an initial response token for initializing the SASL handshake
     * @return the initial response to send to the server, may be null
     */
    public byte[] initialResponse();

    /**
     * Evaluate a challenge received from the Server. Generally, this method
     * should return null when authentication is complete from the client
     * perspective
     * @param challenge the server's SASL challenge
     * @return updated SASL token, may be null to indicate the client
     * requires no further action
     */
    public byte[] evaluateChallenge(byte[] challenge);
}
