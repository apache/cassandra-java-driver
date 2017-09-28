/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.ContactPoints;
import com.datastax.oss.driver.internal.core.DefaultCluster;
import com.datastax.oss.driver.internal.core.config.typesafe.DefaultDriverConfigLoader;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

/**
 * Base implementation to build cluster instances.
 *
 * <p>You only need to deal with this directly if you use custom driver extensions. For the default
 * cluster implementation, see {@link Cluster#builder()}.
 */
public abstract class ClusterBuilder<SelfT extends ClusterBuilder, ClusterT> {

  @SuppressWarnings("unchecked")
  protected final SelfT self = (SelfT) this;

  protected DriverConfigLoader configLoader;
  protected Set<InetSocketAddress> programmaticContactPoints = new HashSet<>();
  protected List<TypeCodec<?>> typeCodecs = new ArrayList<>();

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
   *
   *   <li>the resulting configuration is expected to contain a {@code datastax-java-driver}
   *       section.
   *   <li>that section is validated against the {@link CoreDriverOption core driver options}.
   * </ul>
   *
   * The core driver JAR includes a {@code reference.conf} file with sensible defaults for all
   * mandatory options, except the contact points.
   *
   * @see <a href="https://github.com/typesafehub/config#standard-behavior">Typesafe config's
   *     standard loading behavior</a>
   */
  public SelfT withConfigLoader(DriverConfigLoader configLoader) {
    this.configLoader = configLoader;
    return self;
  }

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
   * they will be merged.
   *
   * <p>Contrary to the configuration, DNS names with multiple A-records will not be handled here.
   * If you need that, call {@link java.net.InetAddress#getAllByName(String)} before calling this
   * method.
   */
  public SelfT addContactPoints(Collection<InetSocketAddress> contactPoints) {
    this.programmaticContactPoints.addAll(contactPoints);
    return self;
  }

  /**
   * Adds a contact point to use for the initial connection to the cluster.
   *
   * @see #addContactPoints(Collection)
   */
  public SelfT addContactPoint(InetSocketAddress contactPoint) {
    this.programmaticContactPoints.add(contactPoint);
    return self;
  }

  /** Registers additional codecs for custom type mappings. */
  public SelfT addTypeCodecs(TypeCodec<?>... typeCodecs) {
    Collections.addAll(this.typeCodecs, typeCodecs);
    return self;
  }

  /**
   * Creates the cluster with the options set by this builder.
   *
   * @return a completion stage that completes with the cluster when it is fully initialized.
   */
  public CompletionStage<ClusterT> buildAsync() {
    return buildDefaultClusterAsync().thenApply(this::wrap);
  }

  /**
   * Convenience method to call {@link #buildAsync()} and block on the result.
   *
   * <p>This must not be called on a driver thread.
   */
  public ClusterT build() {
    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(buildAsync());
  }

  protected abstract ClusterT wrap(Cluster<CqlSession> defaultCluster);

  protected final CompletionStage<Cluster<CqlSession>> buildDefaultClusterAsync() {
    DriverConfigLoader configLoader = buildIfNull(this.configLoader, this::defaultConfigLoader);

    DriverConfigProfile defaultConfig = configLoader.getInitialConfig().getDefaultProfile();
    List<String> configContactPoints =
        defaultConfig.isDefined(CoreDriverOption.CONTACT_POINTS)
            ? defaultConfig.getStringList(CoreDriverOption.CONTACT_POINTS)
            : Collections.emptyList();

    Set<InetSocketAddress> contactPoints =
        ContactPoints.merge(programmaticContactPoints, configContactPoints);

    return DefaultCluster.init(
        (InternalDriverContext) buildContext(configLoader, typeCodecs), contactPoints);
  }

  /**
   * This <b>must</b> return an instance of {@code InternalDriverContext} (it's not expressed
   * directly in the signature to avoid leaking that type through the protected API).
   */
  protected DriverContext buildContext(
      DriverConfigLoader configLoader, List<TypeCodec<?>> typeCodecs) {
    return new DefaultDriverContext(configLoader, typeCodecs);
  }

  private static <T> T buildIfNull(T value, Supplier<T> builder) {
    return (value == null) ? builder.get() : value;
  }
}
