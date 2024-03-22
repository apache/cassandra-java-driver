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

import com.datastax.dse.driver.api.core.auth.BaseDseAuthenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common infrastructure for plain text auth providers.
 *
 * <p>This can be reused to write an implementation that retrieves the credentials from another
 * source than the configuration. The driver offers one built-in implementation: {@link
 * ProgrammaticPlainTextAuthProvider}.
 */
@ThreadSafe
public abstract class PlainTextAuthProviderBase implements AuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(PlainTextAuthProviderBase.class);

  private final String logPrefix;

  /**
   * @param logPrefix a string that will get prepended to the logs (this is used for discrimination
   *     when you have multiple driver instances executing in the same JVM). Built-in
   *     implementations fill this with {@link Session#getName()}.
   */
  protected PlainTextAuthProviderBase(@NonNull String logPrefix) {
    this.logPrefix = Objects.requireNonNull(logPrefix);
  }

  /**
   * Retrieves the credentials from the underlying source.
   *
   * <p>This is invoked every time the driver opens a new connection.
   *
   * @param endPoint The endpoint being contacted.
   * @param serverAuthenticator The authenticator class sent by the endpoint.
   */
  @NonNull
  protected abstract Credentials getCredentials(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator);

  @NonNull
  @Override
  public Authenticator newAuthenticator(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator)
      throws AuthenticationException {
    return new PlainTextAuthenticator(
        getCredentials(endPoint, serverAuthenticator), endPoint, serverAuthenticator);
  }

  @Override
  public void onMissingChallenge(@NonNull EndPoint endPoint) {
    LOG.warn(
        "[{}] {} did not send an authentication challenge; "
            + "This is suspicious because the driver expects authentication",
        logPrefix,
        endPoint);
  }

  @Override
  public void close() {
    // nothing to do
  }

  public static class Credentials {

    private final char[] username;
    private final char[] password;
    private final char[] authorizationId;

    /**
     * Builds an instance for username/password authentication, and proxy authentication with the
     * given authorizationId.
     *
     * <p>This feature is only available with DataStax Enterprise. If the target server is Apache
     * Cassandra, the authorizationId will be ignored.
     */
    public Credentials(
        @NonNull char[] username, @NonNull char[] password, @NonNull char[] authorizationId) {
      this.username = Objects.requireNonNull(username);
      this.password = Objects.requireNonNull(password);
      this.authorizationId = Objects.requireNonNull(authorizationId);
    }

    /** Builds an instance for simple username/password authentication. */
    public Credentials(@NonNull char[] username, @NonNull char[] password) {
      this(username, password, new char[0]);
    }

    @NonNull
    public char[] getUsername() {
      return username;
    }

    /**
     * @deprecated this method only exists for backward compatibility. It is a synonym for {@link
     *     #getUsername()}, which should be used instead.
     */
    @Deprecated
    @NonNull
    public char[] getAuthenticationId() {
      return username;
    }

    @NonNull
    public char[] getPassword() {
      return password;
    }

    @NonNull
    public char[] getAuthorizationId() {
      return authorizationId;
    }

    /** Clears the credentials from memory when they're no longer needed. */
    protected void clear() {
      // Note: this is a bit irrelevant with the built-in provider, because the config already
      // caches the credentials in memory. But it might be useful for a custom implementation that
      // retrieves the credentials from a different source.
      Arrays.fill(getUsername(), (char) 0);
      Arrays.fill(getPassword(), (char) 0);
      Arrays.fill(getAuthorizationId(), (char) 0);
    }
  }

  // Implementation note: BaseDseAuthenticator is backward compatible with Cassandra authenticators.
  // This will work with both Cassandra (as long as no authorizationId is set) and DSE.
  protected static class PlainTextAuthenticator extends BaseDseAuthenticator {

    private static final ByteBuffer MECHANISM =
        ByteBuffer.wrap("PLAIN".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();

    private static final ByteBuffer SERVER_INITIAL_CHALLENGE =
        ByteBuffer.wrap("PLAIN-START".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();

    private static final EndPoint DUMMY_END_POINT =
        new EndPoint() {
          @NonNull
          @Override
          public SocketAddress resolve() {
            return new InetSocketAddress("127.0.0.1", 9042);
          }

          @NonNull
          @Override
          public SocketAddress retrieve() {
            return new InetSocketAddress("127.0.0.1", 9042);
          }

          @NonNull
          @Override
          public String asMetricPrefix() {
            return ""; // will never be used
          }
        };

    private final ByteBuffer encodedCredentials;
    private final EndPoint endPoint;

    protected PlainTextAuthenticator(
        @NonNull Credentials credentials,
        @NonNull EndPoint endPoint,
        @NonNull String serverAuthenticator) {
      super(serverAuthenticator);

      Objects.requireNonNull(credentials);
      Objects.requireNonNull(endPoint);

      ByteBuffer authorizationId = toUtf8Bytes(credentials.getAuthorizationId());
      ByteBuffer username = toUtf8Bytes(credentials.getUsername());
      ByteBuffer password = toUtf8Bytes(credentials.getPassword());

      this.encodedCredentials =
          ByteBuffer.allocate(
              authorizationId.remaining() + username.remaining() + password.remaining() + 2);
      encodedCredentials.put(authorizationId);
      encodedCredentials.put((byte) 0);
      encodedCredentials.put(username);
      encodedCredentials.put((byte) 0);
      encodedCredentials.put(password);
      encodedCredentials.flip();

      clear(authorizationId);
      clear(username);
      clear(password);

      this.endPoint = endPoint;
    }

    /**
     * @deprecated Preserved for backward compatibility, implementors should use the 3-arg
     *     constructor {@code PlainTextAuthenticator(Credentials, EndPoint, String)} instead.
     */
    @Deprecated
    protected PlainTextAuthenticator(@NonNull Credentials credentials) {
      this(
          credentials,
          // It's unlikely that this class was ever extended by third parties, but if it was, assume
          // that it was not written for DSE:
          // - dummy end point because we should never need to build an auth exception
          DUMMY_END_POINT,
          // - default OSS authenticator name (the only thing that matters is how this string
          //   compares to "DseAuthenticator")
          "org.apache.cassandra.auth.PasswordAuthenticator");
    }

    private static ByteBuffer toUtf8Bytes(char[] charArray) {
      CharBuffer charBuffer = CharBuffer.wrap(charArray);
      return Charsets.UTF_8.encode(charBuffer);
    }

    private static void clear(ByteBuffer buffer) {
      buffer.rewind();
      while (buffer.remaining() > 0) {
        buffer.put((byte) 0);
      }
    }

    @NonNull
    @Override
    public ByteBuffer getMechanism() {
      return MECHANISM;
    }

    @NonNull
    @Override
    public ByteBuffer getInitialServerChallenge() {
      return SERVER_INITIAL_CHALLENGE;
    }

    @Nullable
    @Override
    public ByteBuffer evaluateChallengeSync(@Nullable ByteBuffer challenge) {
      if (SERVER_INITIAL_CHALLENGE.equals(challenge)) {
        return encodedCredentials;
      }
      throw new AuthenticationException(endPoint, "Incorrect challenge from server");
    }
  }
}
