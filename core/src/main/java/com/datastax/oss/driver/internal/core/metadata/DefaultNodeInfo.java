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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.Immutable;
import net.jcip.annotations.NotThreadSafe;

@Immutable
public class DefaultNodeInfo implements NodeInfo {
  public static Builder builder() {
    return new Builder();
  }

  private final EndPoint endPoint;
  private final InetSocketAddress broadcastRpcAddress;
  private final InetSocketAddress broadcastAddress;
  private final InetSocketAddress listenAddress;
  private final String datacenter;
  private final String rack;
  private final String cassandraVersion;
  private final String partitioner;
  private final Set<String> tokens;
  private final Map<String, Object> extras;
  private final UUID hostId;
  private final UUID schemaVersion;

  private DefaultNodeInfo(Builder builder) {
    this.endPoint = builder.endPoint;
    this.broadcastRpcAddress = builder.broadcastRpcAddress;
    this.broadcastAddress = builder.broadcastAddress;
    this.listenAddress = builder.listenAddress;
    this.datacenter = builder.datacenter;
    this.rack = builder.rack;
    this.cassandraVersion = builder.cassandraVersion;
    this.partitioner = builder.partitioner;
    this.tokens = (builder.tokens == null) ? Collections.emptySet() : builder.tokens;
    this.hostId = builder.hostId;
    this.schemaVersion = builder.schemaVersion;
    this.extras = (builder.extras == null) ? Collections.emptyMap() : builder.extras;
  }

  @NonNull
  @Override
  public EndPoint getEndPoint() {
    return endPoint;
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastRpcAddress() {
    return Optional.ofNullable(broadcastRpcAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getBroadcastAddress() {
    return Optional.ofNullable(broadcastAddress);
  }

  @NonNull
  @Override
  public Optional<InetSocketAddress> getListenAddress() {
    return Optional.ofNullable(listenAddress);
  }

  @Override
  public String getDatacenter() {
    return datacenter;
  }

  @Override
  public String getRack() {
    return rack;
  }

  @Override
  public String getCassandraVersion() {
    return cassandraVersion;
  }

  @Override
  public String getPartitioner() {
    return partitioner;
  }

  @Override
  public Set<String> getTokens() {
    return tokens;
  }

  @Override
  public Map<String, Object> getExtras() {
    return extras;
  }

  @NonNull
  @Override
  public UUID getHostId() {
    return hostId;
  }

  @Override
  public UUID getSchemaVersion() {
    return schemaVersion;
  }

  @NotThreadSafe
  public static class Builder {
    private EndPoint endPoint;
    private InetSocketAddress broadcastRpcAddress;
    private InetSocketAddress broadcastAddress;
    private InetSocketAddress listenAddress;
    private String datacenter;
    private String rack;
    private String cassandraVersion;
    private String partitioner;
    private Set<String> tokens;
    private Map<String, Object> extras;
    private UUID hostId;
    private UUID schemaVersion;

    public Builder withEndPoint(@NonNull EndPoint endPoint) {
      this.endPoint = endPoint;
      return this;
    }

    public Builder withBroadcastRpcAddress(@Nullable InetSocketAddress address) {
      this.broadcastRpcAddress = address;
      return this;
    }

    public Builder withBroadcastAddress(@Nullable InetSocketAddress address) {
      this.broadcastAddress = address;
      return this;
    }

    public Builder withListenAddress(@Nullable InetSocketAddress address) {
      this.listenAddress = address;
      return this;
    }

    public Builder withDatacenter(@Nullable String datacenter) {
      this.datacenter = datacenter;
      return this;
    }

    public Builder withRack(@Nullable String rack) {
      this.rack = rack;
      return this;
    }

    public Builder withCassandraVersion(@Nullable String cassandraVersion) {
      this.cassandraVersion = cassandraVersion;
      return this;
    }

    public Builder withPartitioner(@Nullable String partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder withTokens(@Nullable Set<String> tokens) {
      this.tokens = tokens;
      return this;
    }

    public Builder withHostId(@NonNull UUID hostId) {
      this.hostId = hostId;
      return this;
    }

    public Builder withSchemaVersion(@Nullable UUID schemaVersion) {
      this.schemaVersion = schemaVersion;
      return this;
    }

    public Builder withExtra(@NonNull String key, @Nullable Object value) {
      if (value != null) {
        if (this.extras == null) {
          this.extras = new HashMap<>();
        }
        this.extras.put(key, value);
      }
      return this;
    }

    public DefaultNodeInfo build() {
      return new DefaultNodeInfo(this);
    }
  }
}
