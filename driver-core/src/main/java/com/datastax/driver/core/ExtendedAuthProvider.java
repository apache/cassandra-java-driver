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
 * An auth provider that represents the host as an {@link EndPoint} instead of a raw {@link
 * InetSocketAddress}.
 *
 * <p>This interface exists solely for backward compatibility: it wasn't possible to change {@link
 * AuthProvider} directly, because it would have broken every 3rd-party implementation.
 *
 * <p>All built-in providers now implement this interface, and it is recommended that new
 * implementations do too.
 *
 * <p>When the driver calls an auth provider, it will check if it implements this interface. If so,
 * it will call {@link #newAuthenticator(EndPoint, String)}; otherwise it will convert the endpoint
 * into an address with {@link EndPoint#resolve()} and call {@link
 * AuthProvider#newAuthenticator(InetSocketAddress, String)}.
 */
public interface ExtendedAuthProvider extends AuthProvider {

  /**
   * The {@code Authenticator} to use when connecting to {@code endpoint}.
   *
   * @param endPoint the Cassandra host to connect to.
   * @param authenticator the configured authenticator on the host.
   * @return The authentication implementation to use.
   */
  Authenticator newAuthenticator(EndPoint endPoint, String authenticator)
      throws AuthenticationException;

  /**
   * @deprecated the driver will never call this method on {@link ExtendedAuthProvider} instances.
   *     Implementors should throw {@link AssertionError}.
   */
  @Override
  @Deprecated
  Authenticator newAuthenticator(InetSocketAddress host, String authenticator)
      throws AuthenticationException;

  class NoAuthProvider implements ExtendedAuthProvider {

    private static final String DSE_AUTHENTICATOR =
        "com.datastax.bdp.cassandra.auth.DseAuthenticator";

    static final String NO_AUTHENTICATOR_MESSAGE =
        "Host %s requires authentication, but no authenticator found in Cluster configuration";

    @Override
    public Authenticator newAuthenticator(EndPoint endPoint, String authenticator) {
      if (authenticator.equals(DSE_AUTHENTICATOR)) {
        return new TransitionalModePlainTextAuthenticator();
      }
      throw new AuthenticationException(
          endPoint, String.format(NO_AUTHENTICATOR_MESSAGE, endPoint));
    }

    @Override
    public Authenticator newAuthenticator(InetSocketAddress host, String authenticator)
        throws AuthenticationException {
      throw new AssertionError(
          "The driver should never call this method on an object that implements "
              + this.getClass().getSimpleName());
    }
  }
}
