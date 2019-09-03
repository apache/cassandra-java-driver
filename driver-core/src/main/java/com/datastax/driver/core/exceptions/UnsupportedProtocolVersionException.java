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
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.ProtocolVersion;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Indicates that we've attempted to connect to a Cassandra node with a protocol version that it
 * cannot handle (e.g., connecting to a C* 1.2 node with protocol version 2).
 */
public class UnsupportedProtocolVersionException extends DriverException
    implements CoordinatorException {

  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;

  private final ProtocolVersion unsupportedVersion;

  private final ProtocolVersion serverVersion;

  public UnsupportedProtocolVersionException(
      EndPoint endPoint, ProtocolVersion unsupportedVersion, ProtocolVersion serverVersion) {
    super(makeErrorMessage(endPoint, unsupportedVersion, serverVersion));
    this.endPoint = endPoint;
    this.unsupportedVersion = unsupportedVersion;
    this.serverVersion = serverVersion;
  }

  public UnsupportedProtocolVersionException(
      EndPoint endPoint,
      ProtocolVersion unsupportedVersion,
      ProtocolVersion serverVersion,
      Throwable cause) {
    super(makeErrorMessage(endPoint, unsupportedVersion, serverVersion), cause);
    this.endPoint = endPoint;
    this.unsupportedVersion = unsupportedVersion;
    this.serverVersion = serverVersion;
  }

  private static String makeErrorMessage(
      EndPoint endPoint, ProtocolVersion unsupportedVersion, ProtocolVersion serverVersion) {
    return unsupportedVersion == serverVersion
        ? String.format(
            "[%s] Host does not support protocol version %s", endPoint, unsupportedVersion)
        : String.format(
            "[%s] Host does not support protocol version %s but %s",
            endPoint, unsupportedVersion, serverVersion);
  }

  @Override
  public EndPoint getEndPoint() {
    return endPoint;
  }

  @Override
  @Deprecated
  public InetSocketAddress getAddress() {
    return (endPoint == null) ? null : endPoint.resolve();
  }

  @Override
  @Deprecated
  public InetAddress getHost() {
    return (endPoint == null) ? null : endPoint.resolve().getAddress();
  }

  /**
   * The version with which the server replied.
   *
   * <p>Note that this version is not necessarily a supported version. While this is usually the
   * case, in rare situations, the server might respond with an unsupported version, to ensure that
   * the client can decode its response properly. See CASSANDRA-11464 for more details.
   *
   * @return The version with which the server replied.
   */
  public ProtocolVersion getServerVersion() {
    return serverVersion;
  }

  /**
   * The version with which the client sent its request.
   *
   * @return The version with which the client sent its request.
   */
  public ProtocolVersion getUnsupportedVersion() {
    return unsupportedVersion;
  }

  @Override
  public UnsupportedProtocolVersionException copy() {
    return new UnsupportedProtocolVersionException(
        endPoint, unsupportedVersion, serverVersion, this);
  }
}
