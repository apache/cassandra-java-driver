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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
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
   */
  ByteBuffer initialResponseSync();

  /**
   * Evaluate a challenge received from the server.
   *
   * <p>{@link #evaluateChallenge(ByteBuffer)} calls this and wraps the result in an immediately
   * completed future.
   */
  ByteBuffer evaluateChallengeSync(ByteBuffer challenge);

  /**
   * Called when authentication is successful with the last information optionally sent by the
   * server.
   *
   * <p>{@link #onAuthenticationSuccess(ByteBuffer)} calls this, and then returns an immediately
   * completed future.
   */
  void onAuthenticationSuccessSync(ByteBuffer token);

  @Override
  default CompletionStage<ByteBuffer> initialResponse() {
    return CompletableFuture.completedFuture(initialResponseSync());
  }

  @Override
  default CompletionStage<ByteBuffer> evaluateChallenge(ByteBuffer challenge) {
    return CompletableFuture.completedFuture(evaluateChallengeSync(challenge));
  }

  @Override
  default CompletionStage<Void> onAuthenticationSuccess(ByteBuffer token) {
    onAuthenticationSuccessSync(token);
    return CompletableFuture.completedFuture(null);
  }
}
