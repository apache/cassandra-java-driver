/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.google.common.base.Charsets;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * A simple {@code AuthProvider} implementation.
 *
 * <p>This provider allows to programmatically define authentication information that will then
 * apply to all hosts. The {@link Authenticator} instances it returns support SASL authentication
 * using the PLAIN mechanism for version 2 (or above) of the CQL native protocol.
 */
public class PlainTextAuthProvider implements AuthProvider {

  private final DriverConfigProfile config;

  /**
   * Create a new simple authentication information provider with the supplied credentials.
   *
   * @param config the configuration of the driver (default profile).
   */
  public PlainTextAuthProvider(DriverConfigProfile config) {
    this.config = config;
  }

  /**
   * Use the supplied credentials and the SASL PLAIN mechanism to login to the server.
   *
   * @param host the Cassandra host with which we want to authenticate.
   * @param serverAuthenticator the configured authenticator on the host.
   * @return an authenticator instance which can be used to perform authentication negotiations on
   *     behalf of the client.
   */
  @Override
  public Authenticator newAuthenticator(SocketAddress host, String serverAuthenticator) {
    String username = config.getString(CoreDriverOption.AUTHENTICATION_CONFIG_USERNAME);
    String password = config.getString(CoreDriverOption.AUTHENTICATION_CONFIG_PASSWORD);
    return new PlainTextAuthenticator(username, password);
  }

  /**
   * Simple implementation of {@link Authenticator} which can perform authentication against
   * Cassandra servers configured with {@code PasswordAuthenticator}.
   */
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
