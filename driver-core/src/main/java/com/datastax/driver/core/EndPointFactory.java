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

/**
 * Produces {@link EndPoint} instances representing the connection information to every node.
 *
 * <p>This component is reserved for advanced use cases where the driver needs more than an IP
 * address to connect.
 *
 * <p>Note that if endpoints do not translate to addresses 1-to-1, the auth provider and SSL options
 * should be instances of {@link ExtendedAuthProvider} and {@link
 * ExtendedRemoteEndpointAwareSslOptions} respectively.
 */
public interface EndPointFactory {

  void init(Cluster cluster);

  /**
   * Creates an instance from a row in {@code system.peers}, or returns {@code null} if there is no
   * sufficient information.
   */
  EndPoint create(Row peersRow);
}
