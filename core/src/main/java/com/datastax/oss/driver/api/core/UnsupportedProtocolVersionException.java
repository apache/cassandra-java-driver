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
package com.datastax.oss.driver.api.core;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;

/**
 * Indicates that we've attempted to connect to a Cassandra node with a protocol version that it
 * cannot handle (e.g., connecting to a C* 2.1 node with protocol version 4).
 */
public class UnsupportedProtocolVersionException extends RuntimeException {
  private static final long serialVersionUID = 0;

  private final SocketAddress address;
  private final List<ProtocolVersion> attemptedVersions;

  public static UnsupportedProtocolVersionException forSingleAttempt(
      SocketAddress address, ProtocolVersion attemptedVersion) {
    String message =
        String.format("[%s] Host does not support protocol version %s", address, attemptedVersion);
    return new UnsupportedProtocolVersionException(
        address, message, Collections.singletonList(attemptedVersion));
  }

  public static UnsupportedProtocolVersionException forNegotiation(
      SocketAddress address, List<ProtocolVersion> attemptedVersions) {
    String message =
        String.format(
            "[%s] Protocol negotiation failed: could not find a common version (attempted: %s)",
            address, attemptedVersions);
    return new UnsupportedProtocolVersionException(
        address, message, ImmutableList.copyOf(attemptedVersions));
  }

  private UnsupportedProtocolVersionException(
      SocketAddress address, String message, List<ProtocolVersion> attemptedVersions) {
    super(message);
    this.address = address;
    this.attemptedVersions = attemptedVersions;
  }

  /** The address of the node that threw the error. */
  public SocketAddress getAddress() {
    return address;
  }

  /** The versions that were attempted. */
  public List<ProtocolVersion> getAttemptedVersions() {
    return attemptedVersions;
  }
}
