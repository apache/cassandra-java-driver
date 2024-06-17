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
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.SyncAuthenticator;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

/**
 * Base class for {@link SyncAuthenticator} implementations that want to make use of the
 * authentication scheme negotiation in <code>DseAuthenticator</code>.
 */
@ThreadSafe
public abstract class BaseDseAuthenticator implements SyncAuthenticator {

  private static final String DSE_AUTHENTICATOR =
      "com.datastax.bdp.cassandra.auth.DseAuthenticator";

  private final String serverAuthenticator;

  protected BaseDseAuthenticator(@NonNull String serverAuthenticator) {
    this.serverAuthenticator = serverAuthenticator;
  }

  /**
   * Return a byte buffer containing the required SASL mechanism.
   *
   * <p>This should be one of:
   *
   * <ul>
   *   <li>PLAIN
   *   <li>GSSAPI
   * </ul>
   *
   * This must be either a {@linkplain ByteBuffer#asReadOnlyBuffer() read-only} buffer, or a new
   * instance every time.
   */
  @NonNull
  protected abstract ByteBuffer getMechanism();

  /**
   * Return a byte buffer containing the expected successful server challenge.
   *
   * <p>This should be one of:
   *
   * <ul>
   *   <li>PLAIN-START
   *   <li>GSSAPI-START
   * </ul>
   *
   * This must be either a {@linkplain ByteBuffer#asReadOnlyBuffer() read-only} buffer, or a new
   * instance every time.
   */
  @NonNull
  protected abstract ByteBuffer getInitialServerChallenge();

  @Nullable
  @Override
  public ByteBuffer initialResponseSync() {
    // DseAuthenticator communicates back the mechanism in response to server authenticate message.
    // older authenticators simply expect the auth response with credentials.
    if (isDseAuthenticator()) {
      return getMechanism();
    } else {
      return evaluateChallengeSync(getInitialServerChallenge());
    }
  }

  @Override
  public void onAuthenticationSuccessSync(@Nullable ByteBuffer token) {}

  private boolean isDseAuthenticator() {
    return serverAuthenticator.equals(DSE_AUTHENTICATOR);
  }
}
