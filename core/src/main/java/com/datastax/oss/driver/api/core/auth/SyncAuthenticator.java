/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core.auth;

import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;

/**
 * An authenticator that performs all of its operations synchronously, on the calling thread.
 *
 * <p>This is intended for simple implementations that are fast and lightweight enough, and do not
 * perform any blocking operations.
 */
public interface SyncAuthenticator extends Authenticator {

  /**
   * Obtain an initial response token for initializing the SASL handshake.
   *
   * <p>{@link #initialResponse()} calls this and wraps the result in an immediately completed
   * future.
   *
   * @return The initial response to send to the server (which may be {@code null}). Note that, if
   *     the returned byte buffer is writable, the driver will <b>clear its contents</b> immediately
   *     after use (to avoid keeping sensitive information in memory); do not reuse the same buffer
   *     across multiple invocations. Alternatively, if the contents are not sensitive, you can make
   *     the buffer {@linkplain ByteBuffer#asReadOnlyBuffer() read-only} and safely reuse it.
   */
  @Nullable
  ByteBuffer initialResponseSync();

  /**
   * Evaluate a challenge received from the server.
   *
   * <p>{@link #evaluateChallenge(ByteBuffer)} calls this and wraps the result in an immediately
   * completed future.
   *
   * @param challenge the server's SASL challenge; may be {@code null}.
   * @return The updated SASL token (which may be {@code null} to indicate the client requires no
   *     further action). Note that, if the returned byte buffer is writable, the driver will
   *     <b>clear its contents</b> immediately after use (to avoid keeping sensitive information in
   *     memory); do not reuse the same buffer across multiple invocations. Alternatively, if the
   *     contents are not sensitive, you can make the buffer {@linkplain
   *     ByteBuffer#asReadOnlyBuffer() read-only} and safely reuse it.
   */
  @Nullable
  ByteBuffer evaluateChallengeSync(@Nullable ByteBuffer challenge);

  /**
   * Called when authentication is successful with the last information optionally sent by the
   * server.
   *
   * <p>{@link #onAuthenticationSuccess(ByteBuffer)} calls this, and then returns an immediately
   * completed future.
   *
   * @param token the information sent by the server with the authentication successful message.
   *     This will be {@code null} if the server sends no particular information on authentication
   *     success.
   */
  void onAuthenticationSuccessSync(@Nullable ByteBuffer token);

  @NonNull
  @Override
  default CompletionStage<ByteBuffer> initialResponse() {
    return CompletableFutures.wrap(this::initialResponseSync);
  }

  @NonNull
  @Override
  default CompletionStage<ByteBuffer> evaluateChallenge(@Nullable ByteBuffer challenge) {
    return CompletableFutures.wrap(() -> evaluateChallengeSync(challenge));
  }

  @NonNull
  @Override
  default CompletionStage<Void> onAuthenticationSuccess(@Nullable ByteBuffer token) {
    return CompletableFutures.wrap(
        () -> {
          onAuthenticationSuccessSync(token);
          return null;
        });
  }
}
