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

/** Indicates an error during the authentication phase while connecting to a node. */
public class AuthenticationException extends DriverException implements CoordinatorException {

  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;

  public AuthenticationException(EndPoint endPoint, String message) {
    super(String.format("Authentication error on host %s: %s", endPoint, message));
    this.endPoint = endPoint;
  }

  // Preserve a constructor with InetSocketAddress for backward compatibility, because legacy
  // authenticators might use it
  public AuthenticationException(InetSocketAddress address, String message) {
    this(new WrappingEndPoint(address), message);
  }

  private AuthenticationException(EndPoint endPoint, String message, Throwable cause) {
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
  public DriverException copy() {
    return new AuthenticationException(endPoint, getMessage(), this);
  }
}
