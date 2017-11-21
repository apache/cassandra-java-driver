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
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;

/**
 * Thrown when the connection on which a request was executing is closed due to an unrelated event.
 *
 * <p>For example, this can happen if the node is unresponsive and a heartbeat query failed, or if
 * the node was forced down.
 *
 * <p>The driver will always retry these requests on the next node transparently. Therefore, the
 * only way to observe this exception is as part of an {@link AllNodesFailedException}.
 */
public class ClosedConnectionException extends DriverException {

  public ClosedConnectionException(String message) {
    this(message, null, false);
  }

  public ClosedConnectionException(String message, Throwable cause) {
    this(message, cause, false);
  }

  private ClosedConnectionException(String message, Throwable cause, boolean writableStackTrace) {
    super(message, cause, writableStackTrace);
  }

  @Override
  public DriverException copy() {
    return new ClosedConnectionException(getMessage(), getCause(), true);
  }
}
