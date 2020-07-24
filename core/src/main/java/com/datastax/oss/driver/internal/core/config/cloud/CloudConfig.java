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
package com.datastax.oss.driver.internal.core.config.cloud;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.util.List;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CloudConfig {

  private final InetSocketAddress proxyAddress;
  private final List<EndPoint> endPoints;
  private final String localDatacenter;
  private final SslEngineFactory sslEngineFactory;

  CloudConfig(
      @NonNull InetSocketAddress proxyAddress,
      @NonNull List<EndPoint> endPoints,
      @NonNull String localDatacenter,
      @NonNull SslEngineFactory sslEngineFactory) {
    this.proxyAddress = proxyAddress;
    this.endPoints = ImmutableList.copyOf(endPoints);
    this.localDatacenter = localDatacenter;
    this.sslEngineFactory = sslEngineFactory;
  }

  @NonNull
  public InetSocketAddress getProxyAddress() {
    return proxyAddress;
  }

  @NonNull
  public List<EndPoint> getEndPoints() {
    return endPoints;
  }

  @NonNull
  public String getLocalDatacenter() {
    return localDatacenter;
  }

  @NonNull
  public SslEngineFactory getSslEngineFactory() {
    return sslEngineFactory;
  }
}
