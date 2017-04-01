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
package com.datastax.oss.driver.internal.core.channel;

import java.net.SocketAddress;

/**
 * Indicates that we've attempted to connect to a node with a cluster name doesn't match that of the
 * other nodes known to the driver.
 *
 * <p>The driver runs the following query on each newly established connection:
 *
 * <pre>
 *     select cluster_name from system.local
 * </pre>
 *
 * The first connection sets the cluster name for this driver instance, all subsequent connections
 * must match it or they will get rejected. This is intended to filter out errors in the discovery
 * process (for example, stale entries in {@code system.peers}).
 */
public class ClusterNameMismatchException extends Exception {

  private static final long serialVersionUID = 0;

  public final SocketAddress address;
  public final String expectedClusterName;
  public final String actualClusterName;

  public ClusterNameMismatchException(
      SocketAddress address, String actualClusterName, String expectedClusterName) {
    super(
        String.format(
            "Host %s reports cluster name '%s' that doesn't match our cluster name '%s'.",
            address, actualClusterName, expectedClusterName));
    this.address = address;
    this.expectedClusterName = expectedClusterName;
    this.actualClusterName = actualClusterName;
  }
}
