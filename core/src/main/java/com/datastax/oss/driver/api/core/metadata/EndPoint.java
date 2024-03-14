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
package com.datastax.oss.driver.api.core.metadata;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Encapsulates the information needed to open connections to a node.
 *
 * <p>By default, the driver assumes plain TCP connections, and this is just a wrapper around an
 * {@link InetSocketAddress}. However, more complex deployment scenarios might use a custom
 * implementation that contains additional information; for example, if the nodes are accessed
 * through a proxy with SNI routing, an SNI server name is needed in addition to the proxy address.
 */
public interface EndPoint {

  /**
   * Resolves this instance to a socket address.
   *
   * <p>This will be called each time the driver opens a new connection to the node. The returned
   * address cannot be null.
   */
  @NonNull
  SocketAddress resolve();

  /**
   * Returns a possibly unresolved instance to a socket address.
   *
   * <p>This should be called when the address does not need to be proactively resolved. For example
   * if the node hostname or port number is needed.
   */
  @NonNull
  SocketAddress retrieve();
  /**
   * Returns an alternate string representation for use in node-level metric names.
   *
   * <p>Because metrics names are path-like, dot-separated strings, raw IP addresses don't make very
   * good identifiers. So this method will typically replace the dots by another character, for
   * example {@code 127_0_0_1_9042}.
   */
  @NonNull
  String asMetricPrefix();
}
