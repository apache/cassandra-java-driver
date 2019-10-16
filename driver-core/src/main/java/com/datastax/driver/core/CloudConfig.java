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
package com.datastax.driver.core;

import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.util.List;

class CloudConfig {

  private final InetSocketAddress proxyAddress;
  private final List<EndPoint> endPoints;
  private final String localDatacenter;
  private final SSLOptions sslOptions;
  private final AuthProvider authProvider;

  CloudConfig(
      InetSocketAddress proxyAddress,
      List<EndPoint> endPoints,
      String localDatacenter,
      SSLOptions sslOptions,
      AuthProvider authProvider) {
    this.proxyAddress = proxyAddress;
    this.endPoints = ImmutableList.copyOf(endPoints);
    this.localDatacenter = localDatacenter;
    this.sslOptions = sslOptions;
    this.authProvider = authProvider;
  }

  /** @return not null proxy Address */
  InetSocketAddress getProxyAddress() {
    return proxyAddress;
  }

  /** @return not null endpoints */
  List<EndPoint> getEndPoints() {
    return endPoints;
  }

  /** @return not null local data center */
  String getLocalDatacenter() {
    return localDatacenter;
  }

  /** @return not null ssl options that can be used to connect to SniProxy */
  SSLOptions getSslOptions() {
    return sslOptions;
  }

  /**
   * @return nullable AuthProvider that can be used to connect to proxy or null if there was not
   *     username/password provided in the secure bundle
   */
  AuthProvider getAuthProvider() {
    return authProvider;
  }
}
