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
package com.datastax.dse.driver.api.core.auth;

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.auth.Authenticator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class DseGssApiAuthProviderBase implements AuthProvider {

  /** The default SASL service name used by this auth provider. */
  public static final String DEFAULT_SASL_SERVICE_NAME = "dse";

  /** The name of the system property to use to specify the SASL service name. */
  public static final String SASL_SERVICE_NAME_PROPERTY = "dse.sasl.service";

  /**
   * Legacy system property for SASL protocol name. Clients should migrate to
   * SASL_SERVICE_NAME_PROPERTY above.
   */
  private static final String LEGACY_SASL_PROTOCOL_PROPERTY = "dse.sasl.protocol";

  private static final Logger LOG = LoggerFactory.getLogger(DseGssApiAuthProviderBase.class);

  private final String logPrefix;

  /**
   * @param logPrefix a string that will get prepended to the logs (this is used for discrimination
   *     when you have multiple driver instances executing in the same JVM). Config-based
   *     implementations fill this with {@link Session#getName()}.
   */
  protected DseGssApiAuthProviderBase(@NonNull String logPrefix) {
    this.logPrefix = Objects.requireNonNull(logPrefix);
  }

  @NonNull
  protected abstract GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator);

  @NonNull
  @Override
  public Authenticator newAuthenticator(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator)
      throws AuthenticationException {
    return new GssApiAuthenticator(
        getOptions(endPoint, serverAuthenticator), endPoint, serverAuthenticator);
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

  /**
   * The options to initialize a new authenticator.
   *
   * <p>Use {@link #builder()} to create an instance.
   */
  @Immutable
  public static class GssApiOptions {

    @NonNull
    public static Builder builder() {
      return new Builder();
    }

    private final Configuration loginConfiguration;
    private final Subject subject;
    private final String saslProtocol;
    private final String authorizationId;
    private final Map<String, String> saslProperties;

    private GssApiOptions(
        @Nullable Configuration loginConfiguration,
        @Nullable Subject subject,
        @Nullable String saslProtocol,
        @Nullable String authorizationId,
        @NonNull Map<String, String> saslProperties) {
      this.loginConfiguration = loginConfiguration;
      this.subject = subject;
      this.saslProtocol = saslProtocol;
      this.authorizationId = authorizationId;
      this.saslProperties = saslProperties;
    }

    @Nullable
    public Configuration getLoginConfiguration() {
      return loginConfiguration;
    }

    @Nullable
    public Subject getSubject() {
      return subject;
    }

    @Nullable
    public String getSaslProtocol() {
      return saslProtocol;
    }

    @Nullable
    public String getAuthorizationId() {
      return authorizationId;
    }

    @NonNull
    public Map<String, String> getSaslProperties() {
      return saslProperties;
    }

    @NotThreadSafe
    public static class Builder {

      private Configuration loginConfiguration;
      private Subject subject;
      private String saslProtocol;
      private String authorizationId;
      private final Map<String, String> saslProperties = new HashMap<>();

      public Builder() {
        saslProperties.put(Sasl.SERVER_AUTH, "true");
        saslProperties.put(Sasl.QOP, "auth");
      }

      /**
       * Sets a login configuration that will be used to create a {@link LoginContext}.
       *
       * <p>You MUST call either a withLoginConfiguration method or {@link #withSubject(Subject)};
       * if both are called, the subject takes precedence, and the login configuration will be
       * ignored.
       *
       * @see #withLoginConfiguration(Map)
       */
      @NonNull
      public Builder withLoginConfiguration(@Nullable Configuration loginConfiguration) {
        this.loginConfiguration = loginConfiguration;
        return this;
      }
      /**
       * Sets a login configuration that will be used to create a {@link LoginContext}.
       *
       * <p>This is an alternative to {@link #withLoginConfiguration(Configuration)}, that builds
       * the configuration from {@code Krb5LoginModule} with the given options.
       *
       * <p>You MUST call either a withLoginConfiguration method or {@link #withSubject(Subject)};
       * if both are called, the subject takes precedence, and the login configuration will be
       * ignored.
       */
      @NonNull
      public Builder withLoginConfiguration(@Nullable Map<String, String> loginConfiguration) {
        this.loginConfiguration = fetchLoginConfiguration(loginConfiguration);
        return this;
      }

      /**
       * Sets a previously authenticated subject to reuse.
       *
       * <p>You MUST call either this method or {@link #withLoginConfiguration(Configuration)}; if
       * both are called, the subject takes precedence, and the login configuration will be ignored.
       */
      @NonNull
      public Builder withSubject(@Nullable Subject subject) {
        this.subject = subject;
        return this;
      }

      /**
       * Sets the SASL protocol name to use; should match the username of the Kerberos service
       * principal used by the DSE server.
       */
      @NonNull
      public Builder withSaslProtocol(@Nullable String saslProtocol) {
        this.saslProtocol = saslProtocol;
        return this;
      }

      /** Sets the authorization ID (allows proxy authentication). */
      @NonNull
      public Builder withAuthorizationId(@Nullable String authorizationId) {
        this.authorizationId = authorizationId;
        return this;
      }

      /**
       * Add a SASL property to use when creating the SASL client.
       *
       * <p>Note that this builder pre-initializes these two default properties:
       *
       * <pre>
       * javax.security.sasl.server.authentication = true
       * javax.security.sasl.qop = auth
       * </pre>
       */
      @NonNull
      public Builder addSaslProperty(@NonNull String name, @NonNull String value) {
        this.saslProperties.put(Objects.requireNonNull(name), Objects.requireNonNull(value));
        return this;
      }

      @NonNull
      public GssApiOptions build() {
        return new GssApiOptions(
            loginConfiguration,
            subject,
            saslProtocol,
            authorizationId,
            ImmutableMap.copyOf(saslProperties));
      }

      public static Configuration fetchLoginConfiguration(Map<String, String> options) {
        return new Configuration() {

          @Override
          public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            return new AppConfigurationEntry[] {
              new AppConfigurationEntry(
                  "com.sun.security.auth.module.Krb5LoginModule",
                  AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                  options)
            };
          }
        };
      }
    }
  }

  protected static class GssApiAuthenticator extends BaseDseAuthenticator {

    private static final ByteBuffer MECHANISM =
        ByteBuffer.wrap("GSSAPI".getBytes(Charsets.UTF_8)).asReadOnlyBuffer();
    private static final ByteBuffer SERVER_INITIAL_CHALLENGE =
        ByteBuffer.wrap("GSSAPI-START".getBytes(Charsets.UTF_8)).asReadOnlyBuffer();
    private static final ByteBuffer EMPTY_BYTE_ARRAY =
        ByteBuffer.wrap(new byte[0]).asReadOnlyBuffer();
    private static final String JAAS_CONFIG_ENTRY = "DseClient";
    private static final String[] SUPPORTED_MECHANISMS = new String[] {"GSSAPI"};

    private Subject subject;
    private SaslClient saslClient;
    private EndPoint endPoint;

    protected GssApiAuthenticator(
        GssApiOptions options, EndPoint endPoint, String serverAuthenticator) {
      super(serverAuthenticator);

      try {
        if (options.getSubject() != null) {
          this.subject = options.getSubject();
        } else {
          Configuration loginConfiguration = options.getLoginConfiguration();
          if (loginConfiguration == null) {
            throw new IllegalArgumentException("Must provide one of subject or loginConfiguration");
          }
          LoginContext login = new LoginContext(JAAS_CONFIG_ENTRY, null, null, loginConfiguration);
          login.login();
          this.subject = login.getSubject();
        }
        String protocol = options.getSaslProtocol();
        if (protocol == null) {
          protocol =
              System.getProperty(
                  SASL_SERVICE_NAME_PROPERTY,
                  System.getProperty(LEGACY_SASL_PROTOCOL_PROPERTY, DEFAULT_SASL_SERVICE_NAME));
        }
        this.saslClient =
            Sasl.createSaslClient(
                SUPPORTED_MECHANISMS,
                options.getAuthorizationId(),
                protocol,
                ((InetSocketAddress) endPoint.resolve()).getAddress().getCanonicalHostName(),
                options.getSaslProperties(),
                null);
      } catch (LoginException | SaslException e) {
        throw new AuthenticationException(endPoint, e.getMessage());
      }
      this.endPoint = endPoint;
    }

    @NonNull
    @Override
    protected ByteBuffer getMechanism() {
      return MECHANISM;
    }

    @NonNull
    @Override
    protected ByteBuffer getInitialServerChallenge() {
      return SERVER_INITIAL_CHALLENGE;
    }

    @Nullable
    @Override
    public ByteBuffer evaluateChallengeSync(@Nullable ByteBuffer challenge) {

      byte[] challengeBytes;
      if (SERVER_INITIAL_CHALLENGE.equals(challenge)) {
        if (!saslClient.hasInitialResponse()) {
          return EMPTY_BYTE_ARRAY;
        }
        challengeBytes = new byte[0];
      } else {
        // The native protocol spec says the incoming challenge can be null depending on the
        // implementation. But saslClient.evaluateChallenge clearly documents that the byte array
        // can't be null, which probably means that a SASL authenticator never sends back null.
        if (challenge == null) {
          throw new AuthenticationException(this.endPoint, "Unexpected null challenge from server");
        }
        challengeBytes = Bytes.getArray(challenge);
      }
      try {

        return ByteBuffer.wrap(
            Subject.doAs(
                subject,
                new PrivilegedExceptionAction<byte[]>() {
                  @Override
                  public byte[] run() throws SaslException {
                    return saslClient.evaluateChallenge(challengeBytes);
                  }
                }));
      } catch (PrivilegedActionException e) {
        throw new AuthenticationException(this.endPoint, e.getMessage(), e.getException());
      }
    }
  }
}
