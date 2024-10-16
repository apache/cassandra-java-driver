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
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.ContactPoints;
import com.datastax.oss.driver.internal.core.config.cloud.CloudConfig;
import com.datastax.oss.driver.internal.core.config.cloud.CloudConfigFactory;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import javax.net.ssl.SSLContext;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(SessionBuilder.class);

  @SuppressWarnings("unchecked")
  protected final SelfT self = (SelfT) this;

  protected DriverConfigLoader configLoader;
  protected Set<EndPoint> programmaticContactPoints = new HashSet<>();
  protected CqlIdentifier keyspace;
  protected Callable<InputStream> cloudConfigInputStream;

  protected ProgrammaticArguments.Builder programmaticArgumentsBuilder =
      ProgrammaticArguments.builder();
  private boolean programmaticSslFactory = false;
  private boolean programmaticLocalDatacenter = false;

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
  @Deprecated
  protected DriverConfigLoader defaultConfigLoader() {
    return new DefaultDriverConfigLoader();
  }

  @NonNull
  protected DriverConfigLoader defaultConfigLoader(@Nullable ClassLoader classLoader) {
    if (classLoader == null) {
      return new DefaultDriverConfigLoader();
    } else {
      return new DefaultDriverConfigLoader(classLoader);
    }
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
   * <p>Listeners can be registered in two ways: either programmatically with this method, or via
   * the configuration using the {@code advanced.metadata.node-state-listener.classes} option.
   *
   * <p>This method unregisters any previously-registered listener. If you intend to register more
   * than one listener, use {@link #addNodeStateListener(NodeStateListener)} instead.
   */
  @NonNull
  public SelfT withNodeStateListener(@Nullable NodeStateListener nodeStateListener) {
    this.programmaticArgumentsBuilder.withNodeStateListener(nodeStateListener);
    return self;
  }

  /**
   * Registers a node state listener to use with the session, without removing previously-registered
   * listeners.
   *
   * <p>Listeners can be registered in two ways: either programmatically with this method, or via
   * the configuration using the {@code advanced.metadata.node-state-listener.classes} option.
   *
   * <p>Unlike {@link #withNodeStateListener(NodeStateListener)}, this method adds the new listener
   * to the list of already-registered listeners, thus allowing applications to register multiple
   * listeners. When multiple listeners are registered, they are notified in sequence every time a
   * new listener event is triggered.
   */
  @NonNull
  public SelfT addNodeStateListener(@NonNull NodeStateListener nodeStateListener) {
    programmaticArgumentsBuilder.addNodeStateListener(nodeStateListener);
    return self;
  }

  /**
   * Registers a schema change listener to use with the session.
   *
   * <p>Listeners can be registered in two ways: either programmatically with this method, or via
   * the configuration using the {@code advanced.metadata.schema-change-listener.classes} option.
   *
   * <p>This method unregisters any previously-registered listener. If you intend to register more
   * than one listener, use {@link #addSchemaChangeListener(SchemaChangeListener)} instead.
   */
  @NonNull
  public SelfT withSchemaChangeListener(@Nullable SchemaChangeListener schemaChangeListener) {
    this.programmaticArgumentsBuilder.withSchemaChangeListener(schemaChangeListener);
    return self;
  }

  /**
   * Registers a schema change listener to use with the session, without removing
   * previously-registered listeners.
   *
   * <p>Listeners can be registered in two ways: either programmatically with this method, or via
   * the configuration using the {@code advanced.metadata.schema-change-listener.classes} option.
   *
   * <p>Unlike {@link #withSchemaChangeListener(SchemaChangeListener)}, this method adds the new
   * listener to the list of already-registered listeners, thus allowing applications to register
   * multiple listeners. When multiple listeners are registered, they are notified in sequence every
   * time a new listener event is triggered.
   */
  @NonNull
  public SelfT addSchemaChangeListener(@NonNull SchemaChangeListener schemaChangeListener) {
    programmaticArgumentsBuilder.addSchemaChangeListener(schemaChangeListener);
    return self;
  }

  /**
   * Registers a request tracker to use with the session.
   *
   * <p>Trackers can be registered in two ways: either programmatically with this method, or via the
   * configuration using the {@code advanced.request-tracker.classes} option.
   *
   * <p>This method unregisters any previously-registered tracker. If you intend to register more
   * than one tracker, use {@link #addRequestTracker(RequestTracker)} instead.
   */
  @NonNull
  public SelfT withRequestTracker(@Nullable RequestTracker requestTracker) {
    this.programmaticArgumentsBuilder.withRequestTracker(requestTracker);
    return self;
  }

  /**
   * Registers a request tracker to use with the session, without removing previously-registered
   * trackers.
   *
   * <p>Trackers can be registered in two ways: either programmatically with this method, or via the
   * configuration using the {@code advanced.request-tracker.classes} option.
   *
   * <p>Unlike {@link #withRequestTracker(RequestTracker)}, this method adds the new tracker to the
   * list of already-registered trackers, thus allowing applications to register multiple trackers.
   * When multiple trackers are registered, they are notified in sequence every time a new tracker
   * event is triggered.
   */
  @NonNull
  public SelfT addRequestTracker(@NonNull RequestTracker requestTracker) {
    programmaticArgumentsBuilder.addRequestTracker(requestTracker);
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
   * Configures the session to use DSE plaintext authentication with the given username and
   * password, and perform proxy authentication with the given authorization id.
   *
   * <p>This feature is only available in DataStax Enterprise. If connecting to Apache Cassandra,
   * the authorization id will be ignored; it is recommended to use {@link
   * #withAuthCredentials(String, String)} instead.
   *
   * <p>This methods calls {@link #withAuthProvider(AuthProvider)} to register a special provider
   * implementation. Therefore calling it overrides the configuration (that is, the {@code
   * advanced.auth-provider.class} option will be ignored).
   *
   * <p>Note that this approach holds the credentials in clear text in memory, which makes them
   * vulnerable to an attacker who is able to perform memory dumps. If this is not acceptable for
   * you, consider writing your own {@link AuthProvider} implementation (the internal class {@code
   * PlainTextAuthProviderBase} is a good starting point), and providing it either with {@link
   * #withAuthProvider(AuthProvider)} or via the configuration ({@code
   * advanced.auth-provider.class}).
   */
  @NonNull
  public SelfT withAuthCredentials(
      @NonNull String username, @NonNull String password, @NonNull String authorizationId) {
    return withAuthProvider(
        new ProgrammaticPlainTextAuthProvider(username, password, authorizationId));
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #withAuthCredentials(String, String)}.
   */
  @Deprecated
  @NonNull
  public SelfT withCredentials(@NonNull String username, @NonNull String password) {
    return withAuthCredentials(username, password);
  }

  /**
   * @deprecated this method only exists to ease the transition from driver 3, it is an alias for
   *     {@link #withAuthCredentials(String, String,String)}.
   */
  @Deprecated
  @NonNull
  public SelfT withCredentials(
      @NonNull String username, @NonNull String password, @NonNull String authorizationId) {
    return withAuthCredentials(username, password, authorizationId);
  }

  /**
   * Registers an SSL engine factory for the session.
   *
   * <p>If the factory is provided programmatically with this method, it overrides the configuration
   * (that is, the {@code advanced.ssl-engine-factory} option will be ignored).
   *
   * @see ProgrammaticSslEngineFactory
   */
  @NonNull
  public SelfT withSslEngineFactory(@Nullable SslEngineFactory sslEngineFactory) {
    this.programmaticSslFactory = true;
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
   * validation. If you need finer control, use {@link #withSslEngineFactory(SslEngineFactory)}
   * directly and pass either your own implementation of {@link SslEngineFactory}, or a {@link
   * ProgrammaticSslEngineFactory} created with custom cipher suites and/or host name validation.
   *
   * <p>Also, note that SSL engines will be created with advisory peer information ({@link
   * SSLContext#createSSLEngine(String, int)}) whenever possible.
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
    this.programmaticLocalDatacenter = true;
    this.programmaticArgumentsBuilder.withLocalDatacenter(profileName, localDatacenter);
    return self;
  }

  /** Alias to {@link #withLocalDatacenter(String, String)} for the default profile. */
  public SelfT withLocalDatacenter(@NonNull String localDatacenter) {
    return withLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME, localDatacenter);
  }

  /**
   * Adds a custom {@link NodeDistanceEvaluator} for a particular execution profile. This assumes
   * that you're also using a dedicated load balancing policy for that profile.
   *
   * <p>Node distance evaluators are honored by all the driver built-in load balancing policies. If
   * you use a custom policy implementation however, you'll need to explicitly invoke the evaluator
   * whenever appropriate.
   *
   * <p>If an evaluator is specified programmatically with this method, it overrides the
   * configuration (that is, the {@code load-balancing-policy.evaluator.class} option will be
   * ignored).
   *
   * @see #withNodeDistanceEvaluator(NodeDistanceEvaluator)
   */
  @NonNull
  public SelfT withNodeDistanceEvaluator(
      @NonNull String profileName, @NonNull NodeDistanceEvaluator nodeDistanceEvaluator) {
    this.programmaticArgumentsBuilder.withNodeDistanceEvaluator(profileName, nodeDistanceEvaluator);
    return self;
  }

  /**
   * Alias to {@link #withNodeDistanceEvaluator(String, NodeDistanceEvaluator)} for the default
   * profile.
   */
  @NonNull
  public SelfT withNodeDistanceEvaluator(@NonNull NodeDistanceEvaluator nodeDistanceEvaluator) {
    return withNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME, nodeDistanceEvaluator);
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
   * <p>Note that this behavior is implemented in the driver built-in load balancing policies. If
   * you use a custom policy implementation, you'll need to explicitly invoke the filter.
   *
   * <p>If the filter is specified programmatically with this method, it overrides the configuration
   * (that is, the {@code load-balancing-policy.filter.class} option will be ignored).
   *
   * <p><strong>This method has been deprecated in favor of {@link
   * #withNodeDistanceEvaluator(String, NodeDistanceEvaluator)}</strong>. If you were using node
   * filters, you can easily replace your filters with the following implementation of {@link
   * NodeDistanceEvaluator}:
   *
   * <pre>{@code
   * public class NodeFilterToDistanceEvaluatorAdapter implements NodeDistanceEvaluator {
   *
   *   private final Predicate<Node> nodeFilter;
   *
   *   public NodeFilterToDistanceEvaluatorAdapter(Predicate<Node> nodeFilter) {
   *     this.nodeFilter = nodeFilter;
   *   }
   *
   *   public NodeDistance evaluateDistance(Node node, String localDc) {
   *     return nodeFilter.test(node) ? null : NodeDistance.IGNORED;
   *   }
   * }
   * }</pre>
   *
   * The same can be achieved using a lambda + closure:
   *
   * <pre>{@code
   * Predicate<Node> nodeFilter = ...
   * NodeDistanceEvaluator evaluator =
   *   (node, localDc) -> nodeFilter.test(node) ? null : NodeDistance.IGNORED;
   * }</pre>
   *
   * @see #withNodeFilter(Predicate)
   * @deprecated Use {@link #withNodeDistanceEvaluator(String, NodeDistanceEvaluator)} instead.
   */
  @Deprecated
  @NonNull
  public SelfT withNodeFilter(@NonNull String profileName, @NonNull Predicate<Node> nodeFilter) {
    this.programmaticArgumentsBuilder.withNodeFilter(profileName, nodeFilter);
    return self;
  }

  /**
   * Alias to {@link #withNodeFilter(String, Predicate)} for the default profile.
   *
   * <p><strong>This method has been deprecated in favor of {@link
   * #withNodeDistanceEvaluator(NodeDistanceEvaluator)}</strong>. See the javadocs of {@link
   * #withNodeFilter(String, Predicate)} to understand how to migrate your legacy node filters.
   *
   * @deprecated Use {@link #withNodeDistanceEvaluator(NodeDistanceEvaluator)} instead.
   */
  @Deprecated
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
   * <p>Unless you define a custom {@link #configLoader}, this class loader will also be used to
   * locate application-specific configuration resources.
   *
   * <p>If you do not provide any custom class loader, the driver will attempt to use the following
   * ones:
   *
   * <ol>
   *   <li>When reflectively loading class names defined in configuration: same class loader that
   *       loaded the core driver classes.
   *   <li>When locating application-specific configuration resources: the current thread's
   *       {@linkplain Thread#getContextClassLoader() context class loader}.
   * </ol>
   *
   * This is generally the right thing to do.
   *
   * <p>Defining a different class loader is typically only needed in web or OSGi environments where
   * there are complex class loading requirements.
   *
   * <p>For example, if the driver jar is loaded by the web server's system class loader (that is,
   * the driver jar was placed in the "/lib" folder of the web server), but the application tries to
   * load a custom load balancing policy declared in the web app's "WEB-INF/lib" folder, the system
   * class loader will not be able to load such class. Instead, you must use the web app's class
   * loader, that you can obtain by calling {@link Thread#getContextClassLoader()}:
   *
   * <pre>{@code
   * CqlSession.builder()
   *   .addContactEndPoint(...)
   *   .withClassLoader(Thread.currentThread().getContextClassLoader())
   *   .build();
   * }</pre>
   *
   * Indeed, in most web environments, {@code Thread.currentThread().getContextClassLoader()} will
   * return the web app's class loader, which is a child of the web server's system class loader.
   * This class loader is thus capable of loading both the implemented interface and the
   * implementing class, in spite of them being declared in different places.
   *
   * <p>For OSGi deployments, it is usually not necessary to use this method. Even if the
   * implemented interface and the implementing class are located in different bundles, the right
   * class loader to use should be the default one (the driver bundle's class loader). In
   * particular, it is not advised to rely on {@code Thread.currentThread().getContextClassLoader()}
   * in OSGi environments, so you should never pass that class loader to this method. See <a
   * href="https://docs.datastax.com/en/developer/java-driver/latest/manual/osgi/#using-a-custom-class-loader">Using
   * a custom ClassLoader</a> in our OSGi online docs for more information.
   */
  @NonNull
  public SelfT withClassLoader(@Nullable ClassLoader classLoader) {
    this.programmaticArgumentsBuilder.withClassLoader(classLoader);
    return self;
  }

  /**
   * Configures this SessionBuilder for Cloud deployments by retrieving connection information from
   * the provided {@link Path}.
   *
   * <p>To connect to a Cloud database, you must first download the secure database bundle from the
   * DataStax Astra console that contains the connection information, then instruct the driver to
   * read its contents using either this method or one if its variants.
   *
   * <p>For more information, please refer to the DataStax Astra documentation.
   *
   * @param cloudConfigPath Path to the secure connect bundle zip file.
   * @see #withCloudSecureConnectBundle(URL)
   * @see #withCloudSecureConnectBundle(InputStream)
   */
  @NonNull
  public SelfT withCloudSecureConnectBundle(@NonNull Path cloudConfigPath) {
    try {
      URL cloudConfigUrl = cloudConfigPath.toAbsolutePath().normalize().toUri().toURL();
      this.cloudConfigInputStream = cloudConfigUrl::openStream;
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Incorrect format of cloudConfigPath", e);
    }
    return self;
  }

  /**
   * Registers a CodecRegistry to use for the session.
   *
   * <p>When both this and {@link #addTypeCodecs(TypeCodec[])} are called, the added type codecs
   * will be registered on the provided CodecRegistry.
   */
  @NonNull
  public SelfT withCodecRegistry(@Nullable MutableCodecRegistry codecRegistry) {
    this.programmaticArgumentsBuilder.withCodecRegistry(codecRegistry);
    return self;
  }

  /**
   * Configures this SessionBuilder for Cloud deployments by retrieving connection information from
   * the provided {@link URL}.
   *
   * <p>To connect to a Cloud database, you must first download the secure database bundle from the
   * DataStax Astra console that contains the connection information, then instruct the driver to
   * read its contents using either this method or one if its variants.
   *
   * <p>For more information, please refer to the DataStax Astra documentation.
   *
   * @param cloudConfigUrl URL to the secure connect bundle zip file.
   * @see #withCloudSecureConnectBundle(Path)
   * @see #withCloudSecureConnectBundle(InputStream)
   */
  @NonNull
  public SelfT withCloudSecureConnectBundle(@NonNull URL cloudConfigUrl) {
    this.cloudConfigInputStream = cloudConfigUrl::openStream;
    return self;
  }

  /**
   * Configures this SessionBuilder for Cloud deployments by retrieving connection information from
   * the provided {@link InputStream}.
   *
   * <p>To connect to a Cloud database, you must first download the secure database bundle from the
   * DataStax Astra console that contains the connection information, then instruct the driver to
   * read its contents using either this method or one if its variants.
   *
   * <p>For more information, please refer to the DataStax Astra documentation.
   *
   * <p>Note that the provided stream will be consumed <em>and closed</em> when either {@link
   * #build()} or {@link #buildAsync()} are called; attempting to reuse it afterwards will result in
   * an error being thrown.
   *
   * @param cloudConfigInputStream A stream containing the secure connect bundle zip file.
   * @see #withCloudSecureConnectBundle(Path)
   * @see #withCloudSecureConnectBundle(URL)
   */
  @NonNull
  public SelfT withCloudSecureConnectBundle(@NonNull InputStream cloudConfigInputStream) {
    this.cloudConfigInputStream = () -> cloudConfigInputStream;
    return self;
  }

  /**
   * Configures this SessionBuilder to use the provided Cloud proxy endpoint.
   *
   * <p>Normally, this method should not be called directly; the normal and easiest way to configure
   * the driver for Cloud deployments is through a {@linkplain #withCloudSecureConnectBundle(URL)
   * secure connect bundle}.
   *
   * <p>Setting this option to any non-null address will make the driver use a special topology
   * monitor tailored for Cloud deployments. This topology monitor assumes that the target cluster
   * should be contacted through the proxy specified here, using SNI routing.
   *
   * <p>For more information, please refer to the DataStax Astra documentation.
   *
   * @param cloudProxyAddress The address of the Cloud proxy to use.
   * @see <a href="https://en.wikipedia.org/wiki/Server_Name_Indication">Server Name Indication</a>
   */
  @NonNull
  public SelfT withCloudProxyAddress(@Nullable InetSocketAddress cloudProxyAddress) {
    this.programmaticArgumentsBuilder.withCloudProxyAddress(cloudProxyAddress);
    return self;
  }

  /**
   * A unique identifier for the created session.
   *
   * <p>It will be sent in the {@code STARTUP} protocol message, under the key {@code CLIENT_ID},
   * for each new connection established by the driver. Currently, this information is used by
   * Insights monitoring (if the target cluster does not support Insights, the entry will be ignored
   * by the server).
   *
   * <p>If you don't call this method, the driver will generate an identifier with {@link
   * Uuids#random()}.
   */
  @NonNull
  public SelfT withClientId(@Nullable UUID clientId) {
    this.programmaticArgumentsBuilder.withStartupClientId(clientId);
    return self;
  }

  /**
   * The name of the application using the created session.
   *
   * <p>It will be sent in the {@code STARTUP} protocol message, under the key {@code
   * APPLICATION_NAME}, for each new connection established by the driver. Currently, this
   * information is used by Insights monitoring (if the target cluster does not support Insights,
   * the entry will be ignored by the server).
   *
   * <p>This can also be defined in the driver configuration with the option {@code
   * basic.application.name}; if you specify both, this method takes precedence and the
   * configuration option will be ignored. If neither is specified, the entry is not included in the
   * message.
   */
  @NonNull
  public SelfT withApplicationName(@Nullable String applicationName) {
    this.programmaticArgumentsBuilder.withStartupApplicationName(applicationName);
    return self;
  }

  /**
   * The version of the application using the created session.
   *
   * <p>It will be sent in the {@code STARTUP} protocol message, under the key {@code
   * APPLICATION_VERSION}, for each new connection established by the driver. Currently, this
   * information is used by Insights monitoring (if the target cluster does not support Insights,
   * the entry will be ignored by the server).
   *
   * <p>This can also be defined in the driver configuration with the option {@code
   * basic.application.version}; if you specify both, this method takes precedence and the
   * configuration option will be ignored. If neither is specified, the entry is not included in the
   * message.
   */
  @NonNull
  public SelfT withApplicationVersion(@Nullable String applicationVersion) {
    this.programmaticArgumentsBuilder.withStartupApplicationVersion(applicationVersion);
    return self;
  }

  /**
   * The metric registry object for storing driver metrics.
   *
   * <p>The argument should be an instance of the base registry type for the metrics framework you
   * are using (see {@code advanced.metrics.factory.class} in the configuration):
   *
   * <ul>
   *   <li>Dropwizard (the default): {@code com.codahale.metrics.MetricRegistry}
   *   <li>Micrometer: {@code io.micrometer.core.instrument.MeterRegistry}
   *   <li>MicroProfile: {@code org.eclipse.microprofile.metrics.MetricRegistry}
   * </ul>
   *
   * Only MicroProfile <em>requires</em> an external instance of its registry to be provided. For
   * Micrometer, if no Registry object is provided, Micrometer's {@code globalRegistry} will be
   * used. For Dropwizard, if no Registry object is provided, an instance of {@code MetricRegistry}
   * will be created and used.
   */
  @NonNull
  public SelfT withMetricRegistry(@Nullable Object metricRegistry) {
    this.programmaticArgumentsBuilder.withMetricRegistry(metricRegistry);
    return self;
  }

  /**
   * Creates the session with the options set by this builder.
   *
   * <p>The session initialization will happen asynchronously in a driver internal thread pool.
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
   * <p>Usage in non-blocking applications: beware that session initialization is a costly
   * operation. It should only be triggered from a thread that is allowed to block. If that is not
   * the case, consider using {@link #buildAsync()} instead.
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

      ProgrammaticArguments programmaticArguments = programmaticArgumentsBuilder.build();

      DriverConfigLoader configLoader =
          this.configLoader != null
              ? this.configLoader
              : defaultConfigLoader(programmaticArguments.getClassLoader());

      DriverExecutionProfile defaultConfig = configLoader.getInitialConfig().getDefaultProfile();
      if (cloudConfigInputStream == null) {
        String configUrlString =
            defaultConfig.getString(DefaultDriverOption.CLOUD_SECURE_CONNECT_BUNDLE, null);
        if (configUrlString != null) {
          cloudConfigInputStream = () -> getURL(configUrlString).openStream();
        }
      }
      List<String> configContactPoints;
      if (defaultConfig.getString(DefaultDriverOption.CONTACT_POINTS).contains(",")) {
        configContactPoints =
            Arrays.asList(
                defaultConfig.getString(DefaultDriverOption.CONTACT_POINTS, "").split(","));
      } else {
        configContactPoints =
            defaultConfig.getStringList(
                DefaultDriverOption.CONTACT_POINTS, Collections.emptyList());
      }
      if (cloudConfigInputStream != null) {
        if (!programmaticContactPoints.isEmpty() || !configContactPoints.isEmpty()) {
          LOG.info(
              "Both a secure connect bundle and contact points were provided. These are mutually exclusive. The contact points from the secure bundle will have priority.");
          // clear the contact points provided in the setting file and via addContactPoints
          configContactPoints = Collections.emptyList();
          programmaticContactPoints = new HashSet<>();
        }

        if (programmaticSslFactory
            || defaultConfig.isDefined(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS)) {
          LOG.info(
              "Both a secure connect bundle and SSL options were provided. They are mutually exclusive. The SSL options from the secure bundle will have priority.");
        }
        CloudConfig cloudConfig =
            new CloudConfigFactory().createCloudConfig(cloudConfigInputStream.call());
        addContactEndPoints(cloudConfig.getEndPoints());

        boolean localDataCenterDefined =
            anyProfileHasDatacenterDefined(configLoader.getInitialConfig());
        if (programmaticLocalDatacenter || localDataCenterDefined) {
          LOG.info(
              "Both a secure connect bundle and a local datacenter were provided. They are mutually exclusive. The local datacenter from the secure bundle will have priority.");
          programmaticArgumentsBuilder.clearDatacenters();
        }
        withLocalDatacenter(cloudConfig.getLocalDatacenter());
        withSslEngineFactory(cloudConfig.getSslEngineFactory());
        withCloudProxyAddress(cloudConfig.getProxyAddress());
        programmaticArguments = programmaticArgumentsBuilder.build();
      }

      boolean resolveAddresses =
          defaultConfig.getBoolean(DefaultDriverOption.RESOLVE_CONTACT_POINTS, true);

      Set<EndPoint> contactPoints =
          ContactPoints.merge(programmaticContactPoints, configContactPoints, resolveAddresses);

      if (keyspace == null && defaultConfig.isDefined(DefaultDriverOption.SESSION_KEYSPACE)) {
        keyspace =
            CqlIdentifier.fromCql(defaultConfig.getString(DefaultDriverOption.SESSION_KEYSPACE));
      }

      return DefaultSession.init(
          (InternalDriverContext) buildContext(configLoader, programmaticArguments),
          contactPoints,
          keyspace);

    } catch (Throwable t) {
      // We construct the session synchronously (until the init() call), but async clients expect a
      // failed future if anything goes wrong. So wrap any error from that synchronous part.
      return CompletableFutures.failedFuture(t);
    }
  }

  private boolean anyProfileHasDatacenterDefined(DriverConfig driverConfig) {
    for (DriverExecutionProfile driverExecutionProfile : driverConfig.getProfiles().values()) {
      if (driverExecutionProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns URL based on the configUrl setting. If the configUrl has no protocol provided, the
   * method will fallback to file:// protocol and return URL that has file protocol specified.
   *
   * @param configUrl url to config secure bundle
   * @return URL with file protocol if there was not explicit protocol provided in the configUrl
   *     setting
   */
  private URL getURL(String configUrl) throws MalformedURLException {
    try {
      return new URL(configUrl);
    } catch (MalformedURLException e1) {
      try {
        return Paths.get(configUrl).toAbsolutePath().normalize().toUri().toURL();
      } catch (MalformedURLException e2) {
        e2.addSuppressed(e1);
        throw e2;
      }
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
}
