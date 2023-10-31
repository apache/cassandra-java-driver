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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Indicates that we've attempted to connect to a Cassandra node with a protocol version that it
 * cannot handle (e.g., connecting to a C* 2.1 node with protocol version 4).
 *
 * <p>The only time when this is returned directly to the client (wrapped in a {@link
 * AllNodesFailedException}) is at initialization. If it happens later when the driver is already
 * connected, it is just logged an the corresponding node is forced down.
 */
public class UnsupportedProtocolVersionException extends DriverException {
  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;
  private final List<ProtocolVersion> attemptedVersions;

  @Nonnull
  public static UnsupportedProtocolVersionException forSingleAttempt(
      @Nonnull EndPoint endPoint, @Nonnull ProtocolVersion attemptedVersion) {
    String message =
        String.format("[%s] Host does not support protocol version %s", endPoint, attemptedVersion);
    return new UnsupportedProtocolVersionException(
        endPoint, message, Collections.singletonList(attemptedVersion), null);
  }

  @Nonnull
  public static UnsupportedProtocolVersionException forNegotiation(
      @Nonnull EndPoint endPoint, @Nonnull List<ProtocolVersion> attemptedVersions) {
    String message =
        String.format(
            "[%s] Protocol negotiation failed: could not find a common version (attempted: %s). "
                + "Note that the driver does not support Cassandra 2.0 or lower.",
            endPoint, attemptedVersions);
    return new UnsupportedProtocolVersionException(
        endPoint, message, ImmutableList.copyOf(attemptedVersions), null);
  }

  public UnsupportedProtocolVersionException(
      @Nullable EndPoint endPoint, // technically nullable, but should never be in real life
      @Nonnull String message,
      @Nonnull List<ProtocolVersion> attemptedVersions) {
    this(endPoint, message, attemptedVersions, null);
  }

  private UnsupportedProtocolVersionException(
      EndPoint endPoint,
      String message,
      List<ProtocolVersion> attemptedVersions,
      ExecutionInfo executionInfo) {
    super(message, executionInfo, null, true);
    this.endPoint = endPoint;
    this.attemptedVersions = attemptedVersions;
  }

  /** The address of the node that threw the error. */
  @Nullable
  public EndPoint getEndPoint() {
    return endPoint;
  }

  /** The versions that were attempted. */
  @Nonnull
  public List<ProtocolVersion> getAttemptedVersions() {
    return attemptedVersions;
  }

  @Nonnull
  @Override
  public DriverException copy() {
    return new UnsupportedProtocolVersionException(
        endPoint, getMessage(), attemptedVersions, getExecutionInfo());
  }
}
