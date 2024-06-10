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
package com.datastax.oss.driver.api.core.auth;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * Handles SASL authentication with Cassandra servers.
 *
 * <p>Each time a new connection is created and the server requires authentication, a new instance
 * of this class will be created by the corresponding {@link AuthProvider} to handle that
 * authentication. The lifecycle of that new {@code Authenticator} will be:
 *
 * <ol>
 *   <li>the {@link #initialResponse} method will be called. The initial return value will be sent
 *       to the server to initiate the handshake.
 *   <li>the server will respond to each client response by either issuing a challenge or indicating
 *       that the authentication is complete (successfully or not). If a new challenge is issued,
 *       the authenticator's {@link #evaluateChallenge} method will be called to produce a response
 *       that will be sent to the server. This challenge/response negotiation will continue until
 *       the server responds that authentication is successful (or an {@link
 *       AuthenticationException} is raised).
 *   <li>When the server indicates that authentication is successful, the {@link
 *       #onAuthenticationSuccess} method will be called with the last information that the server
 *       may optionally have sent.
 * </ol>
 *
 * The exact nature of the negotiation between client and server is specific to the authentication
 * mechanism configured server side.
 *
 * <p>Note that, since the methods in this interface will be invoked on a driver I/O thread, they
 * all return asynchronous results. If your implementation performs heavy computations or blocking
 * calls, you'll want to schedule them on a separate executor, and return a {@code CompletionStage}
 * that represents their future completion. If your implementation is fast, lightweight and does not
 * perform blocking operations, it might be acceptable to run it on I/O threads directly; in that
 * case, implement {@link SyncAuthenticator} instead of this interface.
 */
public interface Authenticator {

  /**
   * Obtain an initial response token for initializing the SASL handshake.
   *
   * @return a completion stage that will complete with the initial response to send to the server
   *     (which may be {@code null}). Note that, if the returned byte buffer is writable, the driver
   *     will <b>clear its contents</b> immediately after use (to avoid keeping sensitive
   *     information in memory); do not reuse the same buffer across multiple invocations.
   *     Alternatively, if the contents are not sensitive, you can make the buffer {@linkplain
   *     ByteBuffer#asReadOnlyBuffer() read-only} and safely reuse it.
   */
  @NonNull
  CompletionStage<ByteBuffer> initialResponse();

  /**
   * Evaluate a challenge received from the server. Generally, this method should return null when
   * authentication is complete from the client perspective.
   *
   * @param challenge the server's SASL challenge.
   * @return a completion stage that will complete with the updated SASL token (which may be null to
   *     indicate the client requires no further action). Note that, if the returned byte buffer is
   *     writable, the driver will <b>clear its contents</b> immediately after use (to avoid keeping
   *     sensitive information in memory); do not reuse the same buffer across multiple invocations.
   *     Alternatively, if the contents are not sensitive, you can make the buffer {@linkplain
   *     ByteBuffer#asReadOnlyBuffer() read-only} and safely reuse it.
   */
  @NonNull
  CompletionStage<ByteBuffer> evaluateChallenge(@Nullable ByteBuffer challenge);

  /**
   * Called when authentication is successful with the last information optionally sent by the
   * server.
   *
   * @param token the information sent by the server with the authentication successful message.
   *     This will be {@code null} if the server sends no particular information on authentication
   *     success.
   * @return a completion stage that completes when the authenticator is done processing this
   *     response.
   */
  @NonNull
  CompletionStage<Void> onAuthenticationSuccess(@Nullable ByteBuffer token);
}
