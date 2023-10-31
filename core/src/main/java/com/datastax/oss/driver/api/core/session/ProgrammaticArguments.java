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

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.loadbalancing.helper.NodeFilterToDistanceEvaluatorAdapter;
import com.datastax.oss.driver.internal.core.metadata.MultiplexingNodeStateListener;
import com.datastax.oss.driver.internal.core.metadata.schema.MultiplexingSchemaChangeListener;
import com.datastax.oss.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The arguments that can be set programmatically when building a session.
 *
 * <p>This is mostly for internal use, you only need to deal with this directly if you write custom
 * {@link SessionBuilder} subclasses.
 */
public class ProgrammaticArguments {

  @Nonnull
  public static Builder builder() {
    return new Builder();
  }

  private final List<TypeCodec<?>> typeCodecs;
  private final NodeStateListener nodeStateListener;
  private final SchemaChangeListener schemaChangeListener;
  private final RequestTracker requestTracker;
  private final Map<String, String> localDatacenters;
  private final Map<String, Predicate<Node>> nodeFilters;
  private final Map<String, NodeDistanceEvaluator> nodeDistanceEvaluators;
  private final ClassLoader classLoader;
  private final AuthProvider authProvider;
  private final SslEngineFactory sslEngineFactory;
  private final InetSocketAddress cloudProxyAddress;
  private final UUID startupClientId;
  private final String startupApplicationName;
  private final String startupApplicationVersion;
  private final MutableCodecRegistry codecRegistry;
  private final Object metricRegistry;

  private ProgrammaticArguments(
      @Nonnull List<TypeCodec<?>> typeCodecs,
      @Nullable NodeStateListener nodeStateListener,
      @Nullable SchemaChangeListener schemaChangeListener,
      @Nullable RequestTracker requestTracker,
      @Nonnull Map<String, String> localDatacenters,
      @Nonnull Map<String, Predicate<Node>> nodeFilters,
      @Nonnull Map<String, NodeDistanceEvaluator> nodeDistanceEvaluators,
      @Nullable ClassLoader classLoader,
      @Nullable AuthProvider authProvider,
      @Nullable SslEngineFactory sslEngineFactory,
      @Nullable InetSocketAddress cloudProxyAddress,
      @Nullable UUID startupClientId,
      @Nullable String startupApplicationName,
      @Nullable String startupApplicationVersion,
      @Nullable MutableCodecRegistry codecRegistry,
      @Nullable Object metricRegistry) {

    this.typeCodecs = typeCodecs;
    this.nodeStateListener = nodeStateListener;
    this.schemaChangeListener = schemaChangeListener;
    this.requestTracker = requestTracker;
    this.localDatacenters = localDatacenters;
    this.nodeFilters = nodeFilters;
    this.nodeDistanceEvaluators = nodeDistanceEvaluators;
    this.classLoader = classLoader;
    this.authProvider = authProvider;
    this.sslEngineFactory = sslEngineFactory;
    this.cloudProxyAddress = cloudProxyAddress;
    this.startupClientId = startupClientId;
    this.startupApplicationName = startupApplicationName;
    this.startupApplicationVersion = startupApplicationVersion;
    this.codecRegistry = codecRegistry;
    this.metricRegistry = metricRegistry;
  }

  @Nonnull
  public List<TypeCodec<?>> getTypeCodecs() {
    return typeCodecs;
  }

  @Nullable
  public NodeStateListener getNodeStateListener() {
    return nodeStateListener;
  }

  @Nullable
  public SchemaChangeListener getSchemaChangeListener() {
    return schemaChangeListener;
  }

  @Nullable
  public RequestTracker getRequestTracker() {
    return requestTracker;
  }

  @Nonnull
  public Map<String, String> getLocalDatacenters() {
    return localDatacenters;
  }

  @Nonnull
  @Deprecated
  @SuppressWarnings("DeprecatedIsStillUsed")
  public Map<String, Predicate<Node>> getNodeFilters() {
    return nodeFilters;
  }

  @Nonnull
  public Map<String, NodeDistanceEvaluator> getNodeDistanceEvaluators() {
    return nodeDistanceEvaluators;
  }

  @Nullable
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Nullable
  public AuthProvider getAuthProvider() {
    return authProvider;
  }

  @Nullable
  public SslEngineFactory getSslEngineFactory() {
    return sslEngineFactory;
  }

  @Nullable
  public InetSocketAddress getCloudProxyAddress() {
    return cloudProxyAddress;
  }

  @Nullable
  public UUID getStartupClientId() {
    return startupClientId;
  }

  @Nullable
  public String getStartupApplicationName() {
    return startupApplicationName;
  }

  @Nullable
  public String getStartupApplicationVersion() {
    return startupApplicationVersion;
  }

  @Nullable
  public MutableCodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @Nullable
  public Object getMetricRegistry() {
    return metricRegistry;
  }

  public static class Builder {

    private final ImmutableList.Builder<TypeCodec<?>> typeCodecsBuilder = ImmutableList.builder();
    private NodeStateListener nodeStateListener;
    private SchemaChangeListener schemaChangeListener;
    private RequestTracker requestTracker;
    private ImmutableMap.Builder<String, String> localDatacentersBuilder = ImmutableMap.builder();
    private final ImmutableMap.Builder<String, Predicate<Node>> nodeFiltersBuilder =
        ImmutableMap.builder();
    private final ImmutableMap.Builder<String, NodeDistanceEvaluator>
        nodeDistanceEvaluatorsBuilder = ImmutableMap.builder();
    private ClassLoader classLoader;
    private AuthProvider authProvider;
    private SslEngineFactory sslEngineFactory;
    private InetSocketAddress cloudProxyAddress;
    private UUID startupClientId;
    private String startupApplicationName;
    private String startupApplicationVersion;
    private MutableCodecRegistry codecRegistry;
    private Object metricRegistry;

    @Nonnull
    public Builder addTypeCodecs(@Nonnull TypeCodec<?>... typeCodecs) {
      this.typeCodecsBuilder.add(typeCodecs);
      return this;
    }

    @Nonnull
    public Builder withNodeStateListener(@Nullable NodeStateListener nodeStateListener) {
      this.nodeStateListener = nodeStateListener;
      return this;
    }

    @Nonnull
    public Builder addNodeStateListener(@Nonnull NodeStateListener nodeStateListener) {
      Objects.requireNonNull(nodeStateListener, "nodeStateListener cannot be null");
      if (this.nodeStateListener == null) {
        this.nodeStateListener = nodeStateListener;
      } else {
        NodeStateListener previousListener = this.nodeStateListener;
        if (previousListener instanceof MultiplexingNodeStateListener) {
          ((MultiplexingNodeStateListener) previousListener).register(nodeStateListener);
        } else {
          MultiplexingNodeStateListener multiplexingNodeStateListener =
              new MultiplexingNodeStateListener();
          multiplexingNodeStateListener.register(previousListener);
          multiplexingNodeStateListener.register(nodeStateListener);
          this.nodeStateListener = multiplexingNodeStateListener;
        }
      }
      return this;
    }

    @Nonnull
    public Builder withSchemaChangeListener(@Nullable SchemaChangeListener schemaChangeListener) {
      this.schemaChangeListener = schemaChangeListener;
      return this;
    }

    @Nonnull
    public Builder addSchemaChangeListener(@Nonnull SchemaChangeListener schemaChangeListener) {
      Objects.requireNonNull(schemaChangeListener, "schemaChangeListener cannot be null");
      if (this.schemaChangeListener == null) {
        this.schemaChangeListener = schemaChangeListener;
      } else {
        SchemaChangeListener previousListener = this.schemaChangeListener;
        if (previousListener instanceof MultiplexingSchemaChangeListener) {
          ((MultiplexingSchemaChangeListener) previousListener).register(schemaChangeListener);
        } else {
          MultiplexingSchemaChangeListener multiplexingSchemaChangeListener =
              new MultiplexingSchemaChangeListener();
          multiplexingSchemaChangeListener.register(previousListener);
          multiplexingSchemaChangeListener.register(schemaChangeListener);
          this.schemaChangeListener = multiplexingSchemaChangeListener;
        }
      }
      return this;
    }

    @Nonnull
    public Builder withRequestTracker(@Nullable RequestTracker requestTracker) {
      this.requestTracker = requestTracker;
      return this;
    }

    @Nonnull
    public Builder addRequestTracker(@Nonnull RequestTracker requestTracker) {
      Objects.requireNonNull(requestTracker, "requestTracker cannot be null");
      if (this.requestTracker == null) {
        this.requestTracker = requestTracker;
      } else {
        RequestTracker previousTracker = this.requestTracker;
        if (previousTracker instanceof MultiplexingRequestTracker) {
          ((MultiplexingRequestTracker) previousTracker).register(requestTracker);
        } else {
          MultiplexingRequestTracker multiplexingRequestTracker = new MultiplexingRequestTracker();
          multiplexingRequestTracker.register(previousTracker);
          multiplexingRequestTracker.register(requestTracker);
          this.requestTracker = multiplexingRequestTracker;
        }
      }
      return this;
    }

    @Nonnull
    public Builder withLocalDatacenter(
        @Nonnull String profileName, @Nonnull String localDatacenter) {
      this.localDatacentersBuilder.put(profileName, localDatacenter);
      return this;
    }

    @Nonnull
    public Builder clearDatacenters() {
      this.localDatacentersBuilder = ImmutableMap.builder();
      return this;
    }

    @Nonnull
    public Builder withLocalDatacenters(Map<String, String> localDatacenters) {
      for (Map.Entry<String, String> entry : localDatacenters.entrySet()) {
        this.localDatacentersBuilder.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    @Nonnull
    public Builder withNodeDistanceEvaluator(
        @Nonnull String profileName, @Nonnull NodeDistanceEvaluator nodeDistanceEvaluator) {
      this.nodeDistanceEvaluatorsBuilder.put(profileName, nodeDistanceEvaluator);
      return this;
    }

    @Nonnull
    public Builder withNodeDistanceEvaluators(
        Map<String, NodeDistanceEvaluator> nodeDistanceReporters) {
      for (Entry<String, NodeDistanceEvaluator> entry : nodeDistanceReporters.entrySet()) {
        this.nodeDistanceEvaluatorsBuilder.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    /**
     * @deprecated Use {@link #withNodeDistanceEvaluator(String, NodeDistanceEvaluator)} instead.
     */
    @Nonnull
    @Deprecated
    public Builder withNodeFilter(
        @Nonnull String profileName, @Nonnull Predicate<Node> nodeFilter) {
      this.nodeFiltersBuilder.put(profileName, nodeFilter);
      this.nodeDistanceEvaluatorsBuilder.put(
          profileName, new NodeFilterToDistanceEvaluatorAdapter(nodeFilter));
      return this;
    }

    /** @deprecated Use {@link #withNodeDistanceEvaluators(Map)} instead. */
    @Nonnull
    @Deprecated
    public Builder withNodeFilters(Map<String, Predicate<Node>> nodeFilters) {
      for (Map.Entry<String, Predicate<Node>> entry : nodeFilters.entrySet()) {
        this.nodeFiltersBuilder.put(entry.getKey(), entry.getValue());
        this.nodeDistanceEvaluatorsBuilder.put(
            entry.getKey(), new NodeFilterToDistanceEvaluatorAdapter(entry.getValue()));
      }
      return this;
    }

    @Nonnull
    public Builder withClassLoader(@Nullable ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @Nonnull
    public Builder withCloudProxyAddress(@Nullable InetSocketAddress cloudAddress) {
      this.cloudProxyAddress = cloudAddress;
      return this;
    }

    @Nonnull
    public Builder withAuthProvider(@Nullable AuthProvider authProvider) {
      this.authProvider = authProvider;
      return this;
    }

    @Nonnull
    public Builder withSslEngineFactory(@Nullable SslEngineFactory sslEngineFactory) {
      this.sslEngineFactory = sslEngineFactory;
      return this;
    }

    @Nonnull
    public Builder withStartupClientId(@Nullable UUID startupClientId) {
      this.startupClientId = startupClientId;
      return this;
    }

    @Nonnull
    public Builder withStartupApplicationName(@Nullable String startupApplicationName) {
      this.startupApplicationName = startupApplicationName;
      return this;
    }

    @Nonnull
    public Builder withStartupApplicationVersion(@Nullable String startupApplicationVersion) {
      this.startupApplicationVersion = startupApplicationVersion;
      return this;
    }

    @Nonnull
    public Builder withCodecRegistry(@Nullable MutableCodecRegistry codecRegistry) {
      this.codecRegistry = codecRegistry;
      return this;
    }

    @Nonnull
    public Builder withMetricRegistry(@Nullable Object metricRegistry) {
      this.metricRegistry = metricRegistry;
      return this;
    }

    @Nonnull
    public ProgrammaticArguments build() {
      return new ProgrammaticArguments(
          typeCodecsBuilder.build(),
          nodeStateListener,
          schemaChangeListener,
          requestTracker,
          localDatacentersBuilder.build(),
          nodeFiltersBuilder.build(),
          nodeDistanceEvaluatorsBuilder.build(),
          classLoader,
          authProvider,
          sslEngineFactory,
          cloudProxyAddress,
          startupClientId,
          startupApplicationName,
          startupApplicationVersion,
          codecRegistry,
          metricRegistry);
    }
  }
}
