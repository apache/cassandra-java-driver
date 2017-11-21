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
package com.datastax.oss.driver.internal.core.metadata;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DefaultNodeInfo implements NodeInfo {
  public static Builder builder() {
    return new Builder();
  }

  private final InetSocketAddress connectAddress;
  private final Optional<InetAddress> broadcastAddress;
  private final Optional<InetAddress> listenAddress;
  private final String datacenter;
  private final String rack;
  private final String cassandraVersion;
  private final String partitioner;
  private final Set<String> tokens;
  private final Map<String, Object> extras;

  private DefaultNodeInfo(Builder builder) {
    this.connectAddress = builder.connectAddress;
    this.broadcastAddress = builder.broadcastAddress;
    this.listenAddress = builder.listenAddress;
    this.datacenter = builder.datacenter;
    this.rack = builder.rack;
    this.cassandraVersion = builder.cassandraVersion;
    this.partitioner = builder.partitioner;
    this.tokens = (builder.tokens == null) ? Collections.emptySet() : builder.tokens;
    this.extras = (builder.extras == null) ? Collections.emptyMap() : builder.extras;
  }

  @Override
  public InetSocketAddress getConnectAddress() {
    return connectAddress;
  }

  @Override
  public Optional<InetAddress> getBroadcastAddress() {
    return broadcastAddress;
  }

  @Override
  public Optional<InetAddress> getListenAddress() {
    return listenAddress;
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

  public static class Builder {
    private InetSocketAddress connectAddress;
    private Optional<InetAddress> broadcastAddress = Optional.empty();
    private Optional<InetAddress> listenAddress = Optional.empty();
    private String datacenter;
    private String rack;
    private String cassandraVersion;
    private String partitioner;
    private Set<String> tokens;
    private Map<String, Object> extras;

    public Builder withConnectAddress(InetSocketAddress address) {
      this.connectAddress = address;
      return this;
    }

    public Builder withBroadcastAddress(InetAddress address) {
      if (address != null) {
        this.broadcastAddress = Optional.of(address);
      }
      return this;
    }

    public Builder withListenAddress(InetAddress address) {
      if (address != null) {
        this.listenAddress = Optional.of(address);
      }
      return this;
    }

    public Builder withDatacenter(String datacenter) {
      this.datacenter = datacenter;
      return this;
    }

    public Builder withRack(String rack) {
      this.rack = rack;
      return this;
    }

    public Builder withCassandraVersion(String cassandraVersion) {
      this.cassandraVersion = cassandraVersion;
      return this;
    }

    public Builder withPartitioner(String partitioner) {
      this.partitioner = partitioner;
      return this;
    }

    public Builder withTokens(Set<String> tokens) {
      this.tokens = tokens;
      return this;
    }

    public Builder withExtra(String key, Object value) {
      if (this.extras == null) {
        this.extras = new HashMap<>();
      }
      this.extras.put(key, value);
      return this;
    }

    public DefaultNodeInfo build() {
      return new DefaultNodeInfo(this);
    }
  }
}
