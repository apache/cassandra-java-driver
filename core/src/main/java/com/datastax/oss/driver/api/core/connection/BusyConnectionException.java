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
 * Indicates that a write was attempted on a connection that already handles too many simultaneous
 * requests.
 *
 * <p>This might happen under heavy load. The driver will automatically try the next node in the
 * query plan. Therefore the only way that the client can observe this exception is as part of a
 * {@link AllNodesFailedException}.
 */
public class BusyConnectionException extends DriverException {

  public BusyConnectionException(int maxAvailableIds) {
    this(
        String.format(
            "Connection has exceeded its maximum of %d simultaneous requests", maxAvailableIds),
        false);
  }

  private BusyConnectionException(String message, boolean writableStackTrace) {
    super(message, null, writableStackTrace);
  }

  @Override
  public DriverException copy() {
    return new BusyConnectionException(getMessage(), true);
  }
}
