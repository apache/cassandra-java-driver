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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.internal.core.util.Strings;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;

/**
 * A simple plaintext {@link AuthProvider} that receives the credentials programmatically instead of
 * pulling them from the configuration.
 *
 * <p>To use this class, create an instance with the appropriate credentials to use and pass it to
 * your session builder:
 *
 * <pre>
 * AuthProvider authProvider = new ProgrammaticPlainTextAuthProvider("...", "...");
 * CqlSession session =
 *     CqlSession.builder()
 *         .addContactEndPoints(...)
 *         .withAuthProvider(authProvider)
 *         .build();
 * </pre>
 *
 * <p>It also offers the possibility of changing the credentials at runtime. The new credentials
 * will be used for all connections initiated after the change.
 *
 * <p>Implementation Note: this implementation is not particularly suited for highly-sensitive
 * applications: it stores the credentials to use as private fields, and even if the fields are char
 * arrays rather than strings to make it difficult to dump their contents, they are never cleared
 * until the provider itself is garbage-collected, which typically only happens when the session is
 * closed.
 *
 * @see SessionBuilder#withAuthProvider(AuthProvider)
 * @see SessionBuilder#withAuthCredentials(String, String)
 * @see SessionBuilder#withAuthCredentials(String, String, String)
 */
@ThreadSafe
public class ProgrammaticPlainTextAuthProvider extends PlainTextAuthProviderBase {

  private volatile char[] username;
  private volatile char[] password;
  private volatile char[] authorizationId;

  /** Builds an instance for simple username/password authentication. */
  public ProgrammaticPlainTextAuthProvider(@NonNull String username, @NonNull String password) {
    this(username, password, "");
  }

  /**
   * Builds an instance for username/password authentication, and proxy authentication with the
   * given authorizationId.
   *
   * <p>This feature is only available with DataStax Enterprise. If the target server is Apache
   * Cassandra, use {@link #ProgrammaticPlainTextAuthProvider(String, String)} instead, or set the
   * authorizationId to an empty string.
   */
  public ProgrammaticPlainTextAuthProvider(
      @NonNull String username, @NonNull String password, @NonNull String authorizationId) {
    // This will typically be built before the session so we don't know the log prefix yet. Pass an
    // empty string, it's only used in one log message.
    super("");
    this.username = Strings.requireNotEmpty(username, "username").toCharArray();
    this.password = Strings.requireNotEmpty(password, "password").toCharArray();
    this.authorizationId =
        Objects.requireNonNull(authorizationId, "authorizationId cannot be null").toCharArray();
  }

  /**
   * Changes the username.
   *
   * <p>The new credentials will be used for all connections initiated after this method was called.
   *
   * @param username the new name.
   */
  public void setUsername(@NonNull String username) {
    this.username = Strings.requireNotEmpty(username, "username").toCharArray();
  }

  /**
   * Changes the password.
   *
   * <p>The new credentials will be used for all connections initiated after this method was called.
   *
   * @param password the new password.
   */
  public void setPassword(@NonNull String password) {
    this.password = Strings.requireNotEmpty(password, "password").toCharArray();
  }

  /**
   * Changes the authorization id.
   *
   * <p>The new credentials will be used for all connections initiated after this method was called.
   *
   * <p>This feature is only available with DataStax Enterprise. If the target server is Apache
   * Cassandra, this method should not be used.
   *
   * @param authorizationId the new authorization id.
   */
  public void setAuthorizationId(@NonNull String authorizationId) {
    this.authorizationId =
        Objects.requireNonNull(authorizationId, "authorizationId cannot be null").toCharArray();
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation disregards the endpoint being connected to as well as the authenticator
   * class sent by the server, and always returns the same credentials.
   */
  @NonNull
  @Override
  protected Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return new Credentials(username.clone(), password.clone(), authorizationId.clone());
  }
}
