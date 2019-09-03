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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.AuthenticationException;
import java.net.InetSocketAddress;

/**
 * Provides {@link Authenticator} instances for use when connecting to Cassandra nodes.
 *
 * <p>See {@link PlainTextAuthProvider} for an implementation which uses SASL PLAIN mechanism to
 * authenticate using username/password strings
 */
public interface AuthProvider {

  /**
   * A provider that provides no authentication capability.
   *
   * <p>This is only useful as a placeholder when no authentication is to be used.
   */
  AuthProvider NONE = new ExtendedAuthProvider.NoAuthProvider();

  /**
   * The {@code Authenticator} to use when connecting to {@code host}
   *
   * @param host the Cassandra host to connect to.
   * @param authenticator the configured authenticator on the host.
   * @return The authentication implementation to use.
   */
  public Authenticator newAuthenticator(InetSocketAddress host, String authenticator)
      throws AuthenticationException;

  /**
   * Dummy Authenticator that accounts for DSE authentication configured with transitional mode.
   *
   * <p>In this situation, the client is allowed to connect without authentication, but DSE would
   * still send an AUTHENTICATE response. This Authenticator handles this situation by sending back
   * a dummy credential.
   */
  class TransitionalModePlainTextAuthenticator
      extends PlainTextAuthProvider.PlainTextAuthenticator {

    public TransitionalModePlainTextAuthenticator() {
      super("", "");
    }
  }
}
