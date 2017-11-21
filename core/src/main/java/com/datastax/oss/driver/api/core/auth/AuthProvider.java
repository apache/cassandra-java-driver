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

import java.net.SocketAddress;

/**
 * Provides {@link Authenticator} instances to use when connecting to Cassandra nodes.
 *
 * <p>See {@link PlainTextAuthProvider} for an implementation which uses SASL PLAIN mechanism to
 * authenticate using username/password strings.
 */
public interface AuthProvider {

  /**
   * The authenticator to use when connecting to {@code host}.
   *
   * @param host the Cassandra host to connect to.
   * @param serverAuthenticator the configured authenticator on the host.
   * @return the authentication implementation to use.
   */
  Authenticator newAuthenticator(SocketAddress host, String serverAuthenticator)
      throws AuthenticationException;
}
