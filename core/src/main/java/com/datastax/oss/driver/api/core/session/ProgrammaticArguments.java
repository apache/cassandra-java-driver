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

import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * The arguments that can be set programmatically when building a session.
 *
 * <p>This is mostly for internal use, you only need to deal with this directly if you write custom
 * {@link SessionBuilder} subclasses.
 */
public class ProgrammaticArguments {

  @NonNull
  public static Builder builder() {
    return new Builder();
  }

  private final List<TypeCodec<?>> typeCodecs;
  private final NodeStateListener nodeStateListener;
  private final SchemaChangeListener schemaChangeListener;
  private final RequestTracker requestTracker;
  private final Map<String, String> localDatacenters;
  private final Map<String, Predicate<Node>> nodeFilters;
  private final ClassLoader classLoader;
  private final AuthProvider authProvider;

  private ProgrammaticArguments(
      @NonNull List<TypeCodec<?>> typeCodecs,
      @Nullable NodeStateListener nodeStateListener,
      @Nullable SchemaChangeListener schemaChangeListener,
      @Nullable RequestTracker requestTracker,
      @NonNull Map<String, String> localDatacenters,
      @NonNull Map<String, Predicate<Node>> nodeFilters,
      @Nullable ClassLoader classLoader,
      @Nullable AuthProvider authProvider) {
    this.typeCodecs = typeCodecs;
    this.nodeStateListener = nodeStateListener;
    this.schemaChangeListener = schemaChangeListener;
    this.requestTracker = requestTracker;
    this.localDatacenters = localDatacenters;
    this.nodeFilters = nodeFilters;
    this.classLoader = classLoader;
    this.authProvider = authProvider;
  }

  @NonNull
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

  @NonNull
  public Map<String, String> getLocalDatacenters() {
    return localDatacenters;
  }

  @NonNull
  public Map<String, Predicate<Node>> getNodeFilters() {
    return nodeFilters;
  }

  @Nullable
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @Nullable
  public AuthProvider getAuthProvider() {
    return authProvider;
  }

  public static class Builder {

    private ImmutableList.Builder<TypeCodec<?>> typeCodecsBuilder = ImmutableList.builder();
    private NodeStateListener nodeStateListener;
    private SchemaChangeListener schemaChangeListener;
    private RequestTracker requestTracker;
    private ImmutableMap.Builder<String, String> localDatacentersBuilder = ImmutableMap.builder();
    private ImmutableMap.Builder<String, Predicate<Node>> nodeFiltersBuilder =
        ImmutableMap.builder();
    private ClassLoader classLoader;
    private AuthProvider authProvider;

    @NonNull
    public Builder addTypeCodecs(@NonNull TypeCodec<?>... typeCodecs) {
      this.typeCodecsBuilder.add(typeCodecs);
      return this;
    }

    @NonNull
    public Builder withNodeStateListener(@Nullable NodeStateListener nodeStateListener) {
      this.nodeStateListener = nodeStateListener;
      return this;
    }

    @NonNull
    public Builder withSchemaChangeListener(@Nullable SchemaChangeListener schemaChangeListener) {
      this.schemaChangeListener = schemaChangeListener;
      return this;
    }

    @NonNull
    public Builder withRequestTracker(@Nullable RequestTracker requestTracker) {
      this.requestTracker = requestTracker;
      return this;
    }

    @NonNull
    public Builder withLocalDatacenter(
        @NonNull String profileName, @NonNull String localDatacenter) {
      this.localDatacentersBuilder.put(profileName, localDatacenter);
      return this;
    }

    @NonNull
    public Builder withLocalDatacenters(Map<String, String> localDatacenters) {
      for (Map.Entry<String, String> entry : localDatacenters.entrySet()) {
        this.localDatacentersBuilder.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    @NonNull
    public Builder withNodeFilter(
        @NonNull String profileName, @NonNull Predicate<Node> nodeFilter) {
      this.nodeFiltersBuilder.put(profileName, nodeFilter);
      return this;
    }

    @NonNull
    public Builder withNodeFilters(Map<String, Predicate<Node>> nodeFilters) {
      for (Map.Entry<String, Predicate<Node>> entry : nodeFilters.entrySet()) {
        this.nodeFiltersBuilder.put(entry.getKey(), entry.getValue());
      }
      return this;
    }

    @NonNull
    public Builder withClassLoader(@Nullable ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    @NonNull
    public Builder withAuthProvider(@Nullable AuthProvider authProvider) {
      this.authProvider = authProvider;
      return this;
    }

    @NonNull
    public ProgrammaticArguments build() {
      return new ProgrammaticArguments(
          typeCodecsBuilder.build(),
          nodeStateListener,
          schemaChangeListener,
          requestTracker,
          localDatacentersBuilder.build(),
          nodeFiltersBuilder.build(),
          classLoader,
          authProvider);
    }
  }
}
