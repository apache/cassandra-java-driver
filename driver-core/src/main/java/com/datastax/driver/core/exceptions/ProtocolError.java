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
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Indicates that the contacted host reported a protocol error. Protocol errors indicate that the
 * client triggered a protocol violation (for instance, a QUERY message is sent before a STARTUP one
 * has been sent). Protocol errors should be considered as a bug in the driver and reported as such.
 */
public class ProtocolError extends DriverInternalError implements CoordinatorException {

  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;

  public ProtocolError(EndPoint endPoint, String message) {
    super(
        String.format(
            "An unexpected protocol error occurred on host %s. This is a bug in this library, please report: %s",
            endPoint, message));
    this.endPoint = endPoint;
  }

  /** Private constructor used solely when copying exceptions. */
  private ProtocolError(EndPoint endPoint, String message, ProtocolError cause) {
    super(message, cause);
    this.endPoint = endPoint;
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

  @Override
  public ProtocolError copy() {
    return new ProtocolError(endPoint, getMessage(), this);
  }
}
