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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.PlainTextAuthProviderBase;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.ContactPoints;
import com.datastax.oss.driver.internal.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.net.ssl.SSLContext;
import net.jcip.annotations.NotThreadSafe;

/**
 * Base implementation to build session instances.
 *
 * <p>You only need to deal with this directly if you use custom driver extensions. For the default
 * session implementation, see {@link CqlSession#builder()}.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public abstract class SessionBuilder<SelfT extends SessionBuilder, SessionT> {

  @SuppressWarnings("unchecked")
  protected final SelfT self = (SelfT) this;

  protected DriverConfigLoader configLoader;
  protected Set<EndPoint> programmaticContactPoints = new HashSet<>();
  protected CqlIdentifier keyspace;

  protected ProgrammaticArguments.Builder programmaticArgumentsBuilder =
      ProgrammaticArguments.builder();

  /**
   * Sets the configuration loader to use.
   *
   * <p>If you don't call this method, the builder will use the default implementation, based on the
   * Typesafe config library. More precisely, configuration properties are loaded and merged from
   * the following (first-listed are higher priority):
   *
   * <ul>
   *   <li>system properties
   *   <li>{@code application.conf} (all resources on classpath with this name)
   *   <li>{@code application.json} (all resources on classpath with this name)
   *   <li>{@code application.properties} (all resources on classpath with this name)
   *   <li>{@code reference.conf} (all resources on classpath with this name). In particular, this
   *       will load the {@code reference.conf} included in the core driver JAR, that defines
   *       default options for all mandatory options.
   * </ul>
   *
   * The resulting configuration is expected to contain a {@code datastax-java-driver} section.
   *
   * <p>This default loader will honor the reload interval defined by the option {@code
   * basic.config-reload-interval}.
   *
   * @see <a href="https://github.com/typesafehub/config#standard-behavior">Typesafe config's
   *     standard loading behavior</a>
   */
  @NonNull
  public SelfT withConfigLoader(@Nullable DriverConfigLoader configLoader) {
    this.configLoader = configLoader;
    return self;
  }

  @NonNull
  protected DriverConfigLoader defaultConfigLoader() {
    return new DefaultDriverConfigLoader();
  }

  /**
   * Adds contact points to use for the initial connection to the cluster.
   *
   * <p>These are addresses of Cassandra nodes that the driver uses to discover the cluster
   * topology. Only one contact point is required (the driver will retrieve the address of the other
   * nodes automatically), but it is usually a good idea to provide more than one contact point,
   * because if that single contact point is unavailable, the driver cannot initialize itself
   * correctly.
   *
   * <p>Contact points can also be provided statically in the configuration. If both are specified,
   * they will be merged. If both are absent, the driver will default to 127.0.0.1:9042.
   *
   * <p>Contrary to the configuration, DNS names with multiple A-records will not be handled here.
   * If you need that, extract them manually with {@link java.net.InetAddress#getAllByName(String)}
   * before calling this method. Similarly, if you need connect addresses to stay unresolved, make
   * sure you pass unresolved instances here (see {@code advanced.resolve-contact-points} in the
   * configuration for more explanations).
   */
  @NonNull
  public SelfT addContactPoints(@NonNull Collection<InetSocketAddress> contactPoints) {
    for (InetSocketAddress contactPoint : contactPoints) {
      addContactPoint(contactPoint);
    }
    return self;
  }

  /**
   * Adds a contact point to use for the initial connection to the cluster.
   *
   * @see #addContactPoints(Collection)
   */
  @NonNull
  public SelfT addContactPoint(@NonNull InetSocketAddress contactPoint) {
    this.programmaticContactPoints.add(new DefaultEndPoint(contactPoint));
    return self;
  }

  /**
   * Adds contact points to use for the initial connection to the cluster.
   *
   * <p>You only need this method if you use a custom {@link EndPoint} implementation. Otherwise,
   * use {@link #addContactPoints(Collection)}.
   */
  @NonNull
  public SelfT addContactEndPoints(@NonNull Collection<EndPoint> contactPoints) {
    for (EndPoint contactPoint : contactPoints) {
      addContactEndPoint(contactPoint);
    }
    return self;
  }

  /**
   * Adds a contact point to use for the initial connection to the cluster.
   *
   * <p>You only need this method if you use a custom {@link EndPoint} implementation. Otherwise,
   * use {@link #addContactPoint(InetSocketAddress)}.
   */
  @NonNull
  public SelfT addContactEndPoint(@NonNull EndPoint contactPoint) {
    this.programmaticContactPoints.add(contactPoint);
    return self;
  }

  /**
   * Registers additional codecs for custom type mappings.
   *
   * @param typeCodecs neither the individual codecs, nor the vararg array itself, can be {@code
   *     null}.
   */
  @NonNull
  public SelfT addTypeCodecs(@NonNull TypeCodec<?>... typeCodecs) {
    this.programmaticArgumentsBuilder.addTypeCodecs(typeCodecs);
    return self;
  }

  /**
   * Registers a node state listener to use with the session.
   *
   * <p>If the listener is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code metadata.node-state-listener.class} option will be ignored).
   */
  @NonNull
  public SelfT withNodeStateListener(@Nullable NodeStateListener nodeStateListener) {
    this.programmaticArgumentsBuilder.withNodeStateListener(nodeStateListener);
    return self;
  }

  /**
   * Registers a schema change listener to use with the session.
   *
   * <p>If the listener is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code metadata.schema-change-listener.class} option will be
   * ignored).
   */
  @NonNull
  public SelfT withSchemaChangeListener(@Nullable SchemaChangeListener schemaChangeListener) {
    this.programmaticArgumentsBuilder.withSchemaChangeListener(schemaChangeListener);
    return self;
  }

  /**
   * Registers a request tracker to use with the session.
   *
   * <p>If the tracker is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code request.tracker.class} option will be ignored).
   */
  @NonNull
  public SelfT withRequestTracker(@Nullable RequestTracker requestTracker) {
    this.programmaticArgumentsBuilder.withRequestTracker(requestTracker);
    return self;
  }

  /**
   * Registers an authentication provider to use with the session.
   *
   * <p>If the provider is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code advanced.auth-provider.class} option will be ignored).
   */
  @NonNull
  public SelfT withAuthProvider(@Nullable AuthProvider authProvider) {
    this.programmaticArgumentsBuilder.withAuthProvider(authProvider);
    return self;
  }

  /**
   * Configures the session to use plaintext authentication with the given username and password.
   *
   * <p>This methods calls {@link #withAuthProvider(AuthProvider)} to register a special provider
   * implementation. Therefore calling it overrides the configuration (that is, the {@code
   * advanced.auth-provider.class} option will be ignored).
   *
   * <p>Note that this approach holds the credentials in clear text in memory, which makes them
   * vulnerable to an attacker who is able to perform memory dumps. If this is not acceptable for
   * you, consider writing your own {@link AuthProvider} implementation ({@link
   * PlainTextAuthProviderBase} is a good starting point), and providing it either with {@link
   * #withAuthProvider(AuthProvider)} or via the configuration ({@code
   * advanced.auth-provider.class}).
   */
  @NonNull
  public SelfT withAuthCredentials(@NonNull String username, @NonNull String password) {
    return withAuthProvider(new ProgrammaticPlainTextAuthProvider(username, password));
  }

  /**
   * Registers an SSL engine factory for the session.
   *
   * <p>If the factory is provided programmatically with this method, it overrides the configuration
   * (that is, the {@code advanced.ssl-engine-factory} option will be ignored).
   */
  @NonNull
  public SelfT withSslEngineFactory(@Nullable SslEngineFactory sslEngineFactory) {
    this.programmaticArgumentsBuilder.withSslEngineFactory(sslEngineFactory);
    return self;
  }

  /**
   * Configures the session to use SSL with the given context.
   *
   * <p>This is a convenience method for clients that already have an {@link SSLContext} instance.
   * It wraps its argument into a {@link ProgrammaticSslEngineFactory}, and passes it to {@link
   * #withSslEngineFactory(SslEngineFactory)}.
   *
   * <p>If you use this method, there is no way to customize cipher suites, or turn on host name
   * validation. Also, note that SSL engines will be created with advisory peer information ({@link
   * SSLContext#createSSLEngine(String, int)}) whenever possible. If you need finer control, write
   * your own factory.
   */
  @NonNull
  public SelfT withSslContext(@Nullable SSLContext sslContext) {
    return withSslEngineFactory(
        sslContext == null ? null : new ProgrammaticSslEngineFactory(sslContext));
  }

  /**
   * Specifies the datacenter that is considered "local" by the load balancing policy.
   *
   * <p>This is a programmatic alternative to the configuration option {@code
   * basic.load-balancing-policy.local-datacenter}. If this method is used, it takes precedence and
   * overrides the configuration.
   *
   * <p>Note that this setting may or may not be relevant depending on the load balancing policy
   * implementation in use. The driver's built-in {@code DefaultLoadBalancingPolicy} relies on it;
   * if you use a third-party implementation, refer to their documentation.
   */
  public SelfT withLocalDatacenter(@NonNull String profileName, @NonNull String localDatacenter) {
    this.programmaticArgumentsBuilder.withLocalDatacenter(profileName, localDatacenter);
    return self;
  }

  /** Alias to {@link #withLocalDatacenter(String, String)} for the default profile. */
  public SelfT withLocalDatacenter(@NonNull String localDatacenter) {
    return withLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME, localDatacenter);
  }

  /**
   * Adds a custom filter to include/exclude nodes for a particular execution profile. This assumes
   * that you're also using a dedicated load balancing policy for that profile.
   *
   * <p>The predicate's {@link Predicate#test(Object) test()} method will be invoked each time the
   * {@link LoadBalancingPolicy} processes a topology or state change: if it returns false, the
   * policy will suggest distance IGNORED (meaning the driver won't ever connect to it if all
   * policies agree), and never included in any query plan.
   *
   * <p>Note that this behavior is implemented in the default load balancing policy. If you use a
   * custom policy implementation, you'll need to explicitly invoke the filter.
   *
   * <p>If the filter is specified programmatically with this method, it overrides the configuration
   * (that is, the {@code load-balancing-policy.filter.class} option will be ignored).
   *
   * @see #withNodeFilter(Predicate)
   */
  @NonNull
  public SelfT withNodeFilter(@NonNull String profileName, @NonNull Predicate<Node> nodeFilter) {
    this.programmaticArgumentsBuilder.withNodeFilter(profileName, nodeFilter);
    return self;
  }

  /** Alias to {@link #withNodeFilter(String, Predicate)} for the default profile. */
  @NonNull
  public SelfT withNodeFilter(@NonNull Predicate<Node> nodeFilter) {
    return withNodeFilter(DriverExecutionProfile.DEFAULT_NAME, nodeFilter);
  }

  /**
   * Sets the keyspace to connect the session to.
   *
   * <p>Note that this can also be provided by the configuration; if both are defined, this method
   * takes precedence.
   */
  @NonNull
  public SelfT withKeyspace(@Nullable CqlIdentifier keyspace) {
    this.keyspace = keyspace;
    return self;
  }

  /**
   * Shortcut for {@link #withKeyspace(CqlIdentifier)
   * setKeyspace(CqlIdentifier.fromCql(keyspaceName))}
   */
  @NonNull
  public SelfT withKeyspace(@Nullable String keyspaceName) {
    return withKeyspace(keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * The {@link ClassLoader} to use to reflectively load class names defined in configuration.
   *
   * <p>This is typically only needed when using OSGi or other in environments where there are
   * complex class loading requirements.
   *
   * <p>If null, the driver attempts to use {@link Thread#getContextClassLoader()} of the current
   * thread or the same {@link ClassLoader} that loaded the core driver classes.
   */
  @NonNull
  public SelfT withClassLoader(@Nullable ClassLoader classLoader) {
    this.programmaticArgumentsBuilder.withClassLoader(classLoader);
    return self;
  }

  /**
   * Creates the session with the options set by this builder.
   *
   * @return a completion stage that completes with the session when it is fully initialized.
   */
  @NonNull
  public CompletionStage<SessionT> buildAsync() {
    CompletionStage<CqlSession> buildStage = buildDefaultSessionAsync();
    CompletionStage<SessionT> wrapStage = buildStage.thenApply(this::wrap);
    // thenApply does not propagate cancellation (!)
    CompletableFutures.propagateCancellation(wrapStage, buildStage);
    return wrapStage;
  }

  /**
   * Convenience method to call {@link #buildAsync()} and block on the result.
   *
   * <p>This must not be called on a driver thread.
   */
  @NonNull
  public SessionT build() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(buildAsync());
  }

  protected abstract SessionT wrap(@NonNull CqlSession defaultSession);

  @NonNull
  protected final CompletionStage<CqlSession> buildDefaultSessionAsync() {
    try {
      DriverConfigLoader configLoader = buildIfNull(this.configLoader, this::defaultConfigLoader);

      DriverExecutionProfile defaultConfig = configLoader.getInitialConfig().getDefaultProfile();
      List<String> configContactPoints =
          defaultConfig.getStringList(DefaultDriverOption.CONTACT_POINTS, Collections.emptyList());
      boolean resolveAddresses =
          defaultConfig.getBoolean(DefaultDriverOption.RESOLVE_CONTACT_POINTS, true);

      Set<EndPoint> contactPoints =
          ContactPoints.merge(programmaticContactPoints, configContactPoints, resolveAddresses);

      if (keyspace == null && defaultConfig.isDefined(DefaultDriverOption.SESSION_KEYSPACE)) {
        keyspace =
            CqlIdentifier.fromCql(defaultConfig.getString(DefaultDriverOption.SESSION_KEYSPACE));
      }

      return DefaultSession.init(
          (InternalDriverContext) buildContext(configLoader, programmaticArgumentsBuilder.build()),
          contactPoints,
          keyspace);

    } catch (Throwable t) {
      // We construct the session synchronously (until the init() call), but async clients expect a
      // failed future if anything goes wrong. So wrap any error from that synchronous part.
      return CompletableFutures.failedFuture(t);
    }
  }

  /**
   * This <b>must</b> return an instance of {@code InternalDriverContext} (it's not expressed
   * directly in the signature to avoid leaking that type through the protected API).
   */
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {

    // Preserve backward compatibility with the deprecated method:
    @SuppressWarnings("deprecation")
    DriverContext legacyApiContext =
        buildContext(
            configLoader,
            programmaticArguments.getTypeCodecs(),
            programmaticArguments.getNodeStateListener(),
            programmaticArguments.getSchemaChangeListener(),
            programmaticArguments.getRequestTracker(),
            programmaticArguments.getLocalDatacenters(),
            programmaticArguments.getNodeFilters(),
            programmaticArguments.getClassLoader());
    if (legacyApiContext != null) {
      return legacyApiContext;
    }

    return new DefaultDriverContext(configLoader, programmaticArguments);
  }

  /**
   * @deprecated this method only exists for backward compatibility (if a subclass written for
   *     driver 4.1.0 returns a non-null result, that value will be used). Please override {@link
   *     #buildContext(DriverConfigLoader, ProgrammaticArguments)} instead.
   */
  @Deprecated
  @SuppressWarnings("DeprecatedIsStillUsed")
  protected DriverContext buildContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    return null;
  }

  private static <T> T buildIfNull(T value, Supplier<T> builder) {
    return (value == null) ? builder.get() : value;
  }
}
