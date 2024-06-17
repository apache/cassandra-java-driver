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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import java.util.Collection;
import java.util.Optional;

/** Defines which native protocol versions are supported by a driver instance. */
public interface ProtocolVersionRegistry {

  /**
   * Look up a version by its {@link ProtocolVersion#name() name}. This is used when a version was
   * forced in the configuration.
   *
   * @throws IllegalArgumentException if there is no known version with this name.
   * @see DefaultDriverOption#PROTOCOL_VERSION
   */
  ProtocolVersion fromName(String name);

  /**
   * The highest, non-beta version supported by the driver. This is used as the starting point for
   * the negotiation process for the initial connection (if the version wasn't forced).
   */
  ProtocolVersion highestNonBeta();

  /**
   * Downgrade to a lower version if the current version is not supported by the server. This is
   * used during the negotiation process for the initial connection (if the version wasn't forced).
   *
   * @return empty if there is no version to downgrade to.
   */
  Optional<ProtocolVersion> downgrade(ProtocolVersion version);

  /**
   * Computes the highest common version supported by the given nodes. This is called after the
   * initial {@link TopologyMonitor#refreshNodeList()} node refresh} (provided that the version was
   * not forced), to ensure that we proceed with a version that will work with all the nodes.
   *
   * @throws UnsupportedProtocolVersionException if no such version exists (the nodes support
   *     non-intersecting ranges), or if there was an error during the computation. This will cause
   *     the driver initialization to fail.
   */
  ProtocolVersion highestCommon(Collection<Node> nodes);

  /** Whether a given version supports a given feature. */
  boolean supports(ProtocolVersion version, ProtocolFeature feature);
}
