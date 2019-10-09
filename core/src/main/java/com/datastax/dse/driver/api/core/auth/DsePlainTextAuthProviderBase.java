/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common infrastructure for DSE plain text auth providers.
 *
 * <p>This can be reused to write an implementation that retrieves the credentials from another
 * source than the configuration.
 */
@ThreadSafe
public abstract class DsePlainTextAuthProviderBase implements AuthProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DsePlainTextAuthProviderBase.class);

  private final String logPrefix;

  /**
   * @param logPrefix a string that will get prepended to the logs (this is used for discrimination
   *     when you have multiple driver instances executing in the same JVM). Config-based
   *     implementations fill this with {@link Session#getName()}.
   */
  protected DsePlainTextAuthProviderBase(@NonNull String logPrefix) {
    this.logPrefix = Objects.requireNonNull(logPrefix);
  }

  /**
   * Retrieves the credentials from the underlying source.
   *
   * <p>This is invoked every time the driver opens a new connection.
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

    private final char[] authenticationId;
    private final char[] password;
    private final char[] authorizationId;

    public Credentials(
        @NonNull char[] authenticationId,
        @NonNull char[] password,
        @NonNull char[] authorizationId) {
      this.authenticationId = Objects.requireNonNull(authenticationId);
      this.password = Objects.requireNonNull(password);
      this.authorizationId = Objects.requireNonNull(authorizationId);
    }

    @NonNull
    public char[] getAuthenticationId() {
      return authenticationId;
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
      Arrays.fill(getAuthenticationId(), (char) 0);
      Arrays.fill(getPassword(), (char) 0);
      Arrays.fill(getAuthorizationId(), (char) 0);
    }
  }

  protected static class PlainTextAuthenticator extends BaseDseAuthenticator {

    private static final ByteBuffer MECHANISM =
        ByteBuffer.wrap("PLAIN".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();

    private static final ByteBuffer SERVER_INITIAL_CHALLENGE =
        ByteBuffer.wrap("PLAIN-START".getBytes(StandardCharsets.UTF_8)).asReadOnlyBuffer();

    private final ByteBuffer encodedCredentials;
    private final EndPoint endPoint;

    protected PlainTextAuthenticator(
        Credentials credentials, EndPoint endPoint, String serverAuthenticator) {
      super(serverAuthenticator);

      Objects.requireNonNull(credentials);

      ByteBuffer authenticationId = toUtf8Bytes(credentials.getAuthenticationId());
      ByteBuffer password = toUtf8Bytes(credentials.getPassword());
      ByteBuffer authorizationId = toUtf8Bytes(credentials.getAuthorizationId());

      this.encodedCredentials =
          ByteBuffer.allocate(
              authorizationId.remaining()
                  + authenticationId.remaining()
                  + password.remaining()
                  + 2);
      encodedCredentials.put(authorizationId);
      encodedCredentials.put((byte) 0);
      encodedCredentials.put(authenticationId);
      encodedCredentials.put((byte) 0);
      encodedCredentials.put(password);
      encodedCredentials.flip();

      clear(authorizationId);
      clear(authenticationId);
      clear(password);

      this.endPoint = endPoint;
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
