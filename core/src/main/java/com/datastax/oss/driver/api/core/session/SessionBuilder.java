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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.ContactPoints;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import net.jcip.annotations.NotThreadSafe;

/**
 * Base implementation to build session instances.
 *
 * <p>You only need to deal with this directly if you use custom driver extensions. For the default
 * session implementation, see {@link CqlSession#builder()}.
 */
@NotThreadSafe
public abstract class SessionBuilder<SelfT extends SessionBuilder, SessionT> {

  @SuppressWarnings("unchecked")
  protected final SelfT self = (SelfT) this;

  protected DriverConfigLoader configLoader;
  protected Set<EndPoint> programmaticContactPoints = new HashSet<>();
  protected List<TypeCodec<?>> typeCodecs = new ArrayList<>();
  private NodeStateListener nodeStateListener;
  private SchemaChangeListener schemaChangeListener;
  protected RequestTracker requestTracker;
  private ImmutableMap.Builder<String, String> localDatacenters = ImmutableMap.builder();
  private ImmutableMap.Builder<String, Predicate<Node>> nodeFilters = ImmutableMap.builder();
  protected CqlIdentifier keyspace;
  private ClassLoader classLoader = null;

  /**
   * Sets the configuration loader to use.
   *
   * <p>If you don't call this method, the builder will use the default implementation, based on the
   * Typesafe config library. More precisely:
   *
   * <ul>
   *   <li>configuration properties are loaded and merged from the following (first-listed are
   *       higher priority):
   *       <ul>
   *         <li>system properties
   *         <li>{@code application.conf} (all resources on classpath with this name)
   *         <li>{@code application.json} (all resources on classpath with this name)
   *         <li>{@code application.properties} (all resources on classpath with this name)
   *         <li>{@code reference.conf} (all resources on classpath with this name)
   *       </ul>
   *   <li>the resulting configuration is expected to contain a {@code datastax-java-driver}
   *       section.
   *   <li>that section is validated against the {@link DefaultDriverOption core driver options}.
   * </ul>
   *
   * The core driver JAR includes a {@code reference.conf} file with sensible defaults for all
   * mandatory options, except the contact points.
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
    Collections.addAll(this.typeCodecs, typeCodecs);
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
    this.nodeStateListener = nodeStateListener;
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
    this.schemaChangeListener = schemaChangeListener;
    return self;
  }

  /**
   * Register a request tracker to use with the session.
   *
   * <p>If the tracker is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code request.tracker.class} option will be ignored).
   */
  @NonNull
  public SelfT withRequestTracker(@Nullable RequestTracker requestTracker) {
    this.requestTracker = requestTracker;
    return self;
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
    this.localDatacenters.put(profileName, localDatacenter);
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
    this.nodeFilters.put(profileName, nodeFilter);
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
    this.classLoader = classLoader;
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
          (InternalDriverContext)
              buildContext(
                  configLoader,
                  typeCodecs,
                  nodeStateListener,
                  schemaChangeListener,
                  requestTracker,
                  localDatacenters.build(),
                  nodeFilters.build(),
                  classLoader),
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
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    return new DefaultDriverContext(
        configLoader,
        typeCodecs,
        nodeStateListener,
        schemaChangeListener,
        requestTracker,
        localDatacenters,
        nodeFilters,
        classLoader);
  }

  private static <T> T buildIfNull(T value, Supplier<T> builder) {
    return (value == null) ? builder.get() : value;
  }
}
