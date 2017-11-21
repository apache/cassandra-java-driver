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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.auth.SyncAuthenticator;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;

/**
 * Dummy authenticator for our tests.
 *
 * <p>The initial response is hard-coded. When the server asks it to evaluate a challenge, it always
 * replies with the same token. When authentication succeeds, the success token is stored for later
 * inspection.
 */
public class MockAuthenticator implements SyncAuthenticator {
  static final String INITIAL_RESPONSE = "0xcafebabe";

  volatile String successToken;

  @Override
  public ByteBuffer initialResponseSync() {
    return Bytes.fromHexString(INITIAL_RESPONSE);
  }

  @Override
  public ByteBuffer evaluateChallengeSync(ByteBuffer challenge) {
    return challenge;
  }

  @Override
  public void onAuthenticationSuccessSync(ByteBuffer token) {
    successToken = Bytes.toHexString(token);
  }
}
