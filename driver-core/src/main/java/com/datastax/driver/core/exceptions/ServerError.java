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
 * Indicates that the contacted host reported an internal error. This should be considered as a bug
 * in Cassandra and reported as such.
 */
public class ServerError extends DriverInternalError implements CoordinatorException {

  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;

  public ServerError(EndPoint endPoint, String message) {
    super(String.format("An unexpected error occurred server side on %s: %s", endPoint, message));
    this.endPoint = endPoint;
  }

  /** Private constructor used solely when copying exceptions. */
  private ServerError(EndPoint endPoint, String message, ServerError cause) {
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
  public ServerError copy() {
    return new ServerError(endPoint, getMessage(), this);
  }
}
