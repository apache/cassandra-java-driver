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
package com.datastax.oss.driver.internal.core.config.cloud;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.List;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CloudConfig {

  private final InetSocketAddress proxyAddress;
  private final List<EndPoint> endPoints;
  private final String localDatacenter;
  private final SslEngineFactory sslEngineFactory;

  CloudConfig(
      @Nonnull InetSocketAddress proxyAddress,
      @Nonnull List<EndPoint> endPoints,
      @Nonnull String localDatacenter,
      @Nonnull SslEngineFactory sslEngineFactory) {
    this.proxyAddress = proxyAddress;
    this.endPoints = ImmutableList.copyOf(endPoints);
    this.localDatacenter = localDatacenter;
    this.sslEngineFactory = sslEngineFactory;
  }

  @Nonnull
  public InetSocketAddress getProxyAddress() {
    return proxyAddress;
  }

  @Nonnull
  public List<EndPoint> getEndPoints() {
    return endPoints;
  }

  @Nonnull
  public String getLocalDatacenter() {
    return localDatacenter;
  }

  @Nonnull
  public SslEngineFactory getSslEngineFactory() {
    return sslEngineFactory;
  }
}
