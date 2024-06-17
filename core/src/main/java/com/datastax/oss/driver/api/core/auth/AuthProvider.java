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

import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Provides {@link Authenticator} instances to use when connecting to Cassandra nodes.
 *
 * <p>See {@link PlainTextAuthProvider} for an implementation which uses SASL PLAIN mechanism to
 * authenticate using username/password strings.
 */
public interface AuthProvider extends AutoCloseable {

  /**
   * The authenticator to use when connecting to {@code host}.
   *
   * @param endPoint the Cassandra host to connect to.
   * @param serverAuthenticator the configured authenticator on the host.
   * @return the authentication implementation to use.
   */
  @NonNull
  Authenticator newAuthenticator(@NonNull EndPoint endPoint, @NonNull String serverAuthenticator)
      throws AuthenticationException;

  /**
   * What to do if the server does not send back an authentication challenge (in other words, lets
   * the client connect without any form of authentication).
   *
   * <p>This is suspicious because having authentication enabled on the client but not on the server
   * is probably a configuration mistake.
   *
   * <p>Provider implementations are free to handle this however they want; typical approaches are:
   *
   * <ul>
   *   <li>ignoring;
   *   <li>logging a warning;
   *   <li>throwing an {@link AuthenticationException} to abort the connection (but note that it
   *       will be retried according to the {@link ReconnectionPolicy}).
   * </ul>
   */
  void onMissingChallenge(@NonNull EndPoint endPoint) throws AuthenticationException;
}
