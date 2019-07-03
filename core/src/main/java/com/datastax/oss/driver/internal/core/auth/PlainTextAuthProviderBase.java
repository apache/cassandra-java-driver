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
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common infrastructure for plain text auth providers.
 *
 * <p>This can be reused to write an implementation that retrieves the credentials from another
 * source than the configuration.
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
   */
  @NonNull
  protected abstract Credentials getCredentials();

  @NonNull
  @Override
  public Authenticator newAuthenticator(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return new PlainTextAuthenticator(getCredentials());
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

  protected static class Credentials {

    private final char[] username;
    private final char[] password;

    public Credentials(@NonNull char[] username, @NonNull char[] password) {
      this.username = Objects.requireNonNull(username);
      this.password = Objects.requireNonNull(password);
    }

    @NonNull
    public char[] getUsername() {
      return username;
    }

    @NonNull
    public char[] getPassword() {
      return password;
    }

    /** Clears the credentials from memory when they're no longer needed. */
    protected void clear() {
      // Note: this is a bit irrelevant with the built-in provider, because the config already
      // caches the credentials in memory. But it might be useful for a custom implementation that
      // retrieves the credentials from a different source.
      Arrays.fill(getUsername(), (char) 0);
      Arrays.fill(getPassword(), (char) 0);
    }
  }

  protected static class PlainTextAuthenticator implements SyncAuthenticator {

    private final ByteBuffer initialToken;

    protected PlainTextAuthenticator(@NonNull Credentials credentials) {
      Objects.requireNonNull(credentials);
      ByteBuffer usernameBytes = toUtf8Bytes(credentials.getUsername());
      ByteBuffer passwordBytes = toUtf8Bytes(credentials.getPassword());
      credentials.clear();

      this.initialToken =
          ByteBuffer.allocate(usernameBytes.remaining() + passwordBytes.remaining() + 2);
      initialToken.put((byte) 0);
      initialToken.put(usernameBytes);
      initialToken.put((byte) 0);
      initialToken.put(passwordBytes);
      initialToken.flip();

      // Clear temporary buffers
      usernameBytes.rewind();
      while (usernameBytes.remaining() > 0) {
        usernameBytes.put((byte) 0);
      }
      passwordBytes.rewind();
      while (passwordBytes.remaining() > 0) {
        passwordBytes.put((byte) 0);
      }
    }

    private ByteBuffer toUtf8Bytes(char[] charArray) {
      CharBuffer charBuffer = CharBuffer.wrap(charArray);
      return Charsets.UTF_8.encode(charBuffer);
    }

    @Override
    @Nullable
    public ByteBuffer initialResponseSync() {
      return initialToken;
    }

    @Override
    @Nullable
    public ByteBuffer evaluateChallengeSync(@Nullable ByteBuffer token) {
      return null;
    }

    @Override
    public void onAuthenticationSuccessSync(@Nullable ByteBuffer token) {
      // no-op, the server should send nothing anyway
    }
  }
}
