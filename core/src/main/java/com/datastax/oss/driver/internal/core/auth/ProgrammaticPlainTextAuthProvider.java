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
package com.datastax.oss.driver.internal.core.auth;

import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

/**
 * Alternative plaintext auth provider that receives the credentials programmatically instead of
 * pulling them from the configuration.
 *
 * @see SessionBuilder#withAuthCredentials(String, String)
 * @see SessionBuilder#withAuthCredentials(String, String, String)
 */
@ThreadSafe
public class ProgrammaticPlainTextAuthProvider extends PlainTextAuthProviderBase {
  private final String username;
  private final String password;
  private final String authorizationId;

  /** Builds an instance for simple username/password authentication. */
  public ProgrammaticPlainTextAuthProvider(String username, String password) {
    this(username, password, "");
  }

  /**
   * Builds an instance for username/password authentication, and proxy authentication with the
   * given authorizationId.
   *
   * <p>This feature is only available with Datastax Enterprise. If the target server is Apache
   * Cassandra, the authorizationId will be ignored.
   */
  public ProgrammaticPlainTextAuthProvider(
      String username, String password, String authorizationId) {
    // This will typically be built before the session so we don't know the log prefix yet. Pass an
    // empty string, it's only used in one log message.
    super("");
    this.username = username;
    this.password = password;
    this.authorizationId = authorizationId;
  }

  @NonNull
  @Override
  protected Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return new Credentials(
        username.toCharArray(), password.toCharArray(), authorizationId.toCharArray());
  }
}
