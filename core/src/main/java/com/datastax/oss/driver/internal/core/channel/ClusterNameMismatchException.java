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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.metadata.EndPoint;

/**
 * Indicates that we've attempted to connect to a node with a cluster name that doesn't match that
 * of the other nodes known to the driver.
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
 *
 * <p>This error is never returned directly to the client. If we detect a mismatch, it will always
 * be after the driver has connected successfully; the error will be logged and the offending node
 * forced down.
 */
public class ClusterNameMismatchException extends RuntimeException {

  private static final long serialVersionUID = 0;

  public final EndPoint endPoint;
  public final String expectedClusterName;
  public final String actualClusterName;

  public ClusterNameMismatchException(
      EndPoint endPoint, String actualClusterName, String expectedClusterName) {
    super(
        String.format(
            "Node %s reports cluster name '%s' that doesn't match our cluster name '%s'. "
                + "It will be forced down.",
            endPoint, actualClusterName, expectedClusterName));
    this.endPoint = endPoint;
    this.expectedClusterName = expectedClusterName;
    this.actualClusterName = actualClusterName;
  }
}
