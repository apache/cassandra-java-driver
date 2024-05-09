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
package com.datastax.oss.driver.api.core.loadbalancing;
/**
 * Determines how the driver will manage connections to a Cassandra node.
 *
 * <p>The distance is assigned by a {@link LoadBalancingPolicy}.
 */
public enum NodeDistance {
  /**
   * An "active" distance that, indicates that the driver should maintain connections to the node;
   * it also marks it as "preferred", meaning that the number or capacity of the connections may be
   * higher, and that the node may also have priority for some tasks (for example, being chosen as
   * the control host).
   */
  LOCAL,
  /**
   * An "active" distance that, indicates that the driver should maintain connections to the node;
   * it also marks it as "less preferred", meaning that the number or capacity of the connections
   * may be lower, and that other nodes may have a higher priority for some tasks (for example,
   * being chosen as the control host).
   */
  REMOTE,
  /**
   * An "inactive" distance, that indicates that the driver will not open any connection to the
   * node.
   */
  IGNORED,
}
