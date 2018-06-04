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
package com.datastax.oss.driver.internal.core.auth;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.auth.SyncAuthenticator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple authentication provider that supports SASL authentication using the PLAIN mechanism for
 * version 3 (or above) of the CQL native protocol.
 *
 * <p>To activate this provider, an {@code auth-provider} section must be included in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.auth-provider {
 *     class = com.datastax.driver.api.core.auth.PlainTextAuthProvider
 *     username = cassandra
 *     password = cassandra
 *   }
 * }
 * </pre>
 *
 * See the {@code reference.conf} file included with the driver for more information.
 */
@ThreadSafe
public class PlainTextAuthProvider implements AuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(PlainTextAuthProvider.class);

  private final String logPrefix;
  private final DriverConfigProfile config;

  /** Builds a new instance. */
  public PlainTextAuthProvider(DriverContext context) {
    this.logPrefix = context.sessionName();
    this.config = context.config().getDefaultProfile();
  }

  @Override
  public Authenticator newAuthenticator(SocketAddress host, String serverAuthenticator) {
    String username = config.getString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME);
    String password = config.getString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD);
    return new PlainTextAuthenticator(username, password);
  }

  @Override
  public void onMissingChallenge(SocketAddress host) {
    LOG.warn(
        "[{}] {} did not send an authentication challenge; "
            + "This is suspicious because the driver expects authentication",
        logPrefix,
        host);
  }

  private static class PlainTextAuthenticator implements SyncAuthenticator {

    private final ByteBuffer initialToken;

    PlainTextAuthenticator(String username, String password) {
      byte[] usernameBytes = username.getBytes(Charsets.UTF_8);
      byte[] passwordBytes = password.getBytes(Charsets.UTF_8);
      this.initialToken = ByteBuffer.allocate(usernameBytes.length + passwordBytes.length + 2);
      initialToken.put((byte) 0);
      initialToken.put(usernameBytes);
      initialToken.put((byte) 0);
      initialToken.put(passwordBytes);
      initialToken.flip();
    }

    @Override
    public ByteBuffer initialResponseSync() {
      return initialToken.duplicate();
    }

    @Override
    public ByteBuffer evaluateChallengeSync(ByteBuffer token) {
      return null;
    }

    @Override
    public void onAuthenticationSuccessSync(ByteBuffer token) {
      // no-op, the server should send nothing anyway
    }
  }
}
