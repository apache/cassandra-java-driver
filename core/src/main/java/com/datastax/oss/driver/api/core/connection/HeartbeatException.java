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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.DriverException;
import java.net.SocketAddress;

/**
 * Thrown when a heartbeat query fails.
 *
 * <p>Heartbeat queries are sent automatically on idle connections, to ensure that they are still
 * alive. If a heartbeat query fails, the connection is closed, and all pending queries are aborted.
 * Depending on the retry policy, the heartbeat exception can either be rethrown directly to the
 * client, or the driver tries the next host in the query plan.
 */
public class HeartbeatException extends DriverException {

  private final SocketAddress address;

  public HeartbeatException(SocketAddress address, String message, Throwable cause) {
    super(message, cause, true);
    this.address = address;
  }

  /** The address of the node that encountered the error. */
  public SocketAddress getAddress() {
    return address;
  }

  @Override
  public DriverException copy() {
    return new HeartbeatException(address, getMessage(), getCause());
  }
}
