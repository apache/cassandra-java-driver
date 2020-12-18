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
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Indicates a generic error while initializing a connection.
 *
 * <p>The only time when this is returned directly to the client (wrapped in a {@link
 * AllNodesFailedException}) is at initialization. If it happens later when the driver is already
 * connected, it is just logged and the connection is reattempted.
 */
public class ConnectionInitException extends DriverException {
  public ConnectionInitException(@NonNull String message, @Nullable Throwable cause) {
    super(message, null, cause, true);
  }

  private ConnectionInitException(String message, ExecutionInfo executionInfo, Throwable cause) {
    super(message, executionInfo, cause, true);
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new ConnectionInitException(getMessage(), getExecutionInfo(), getCause());
  }
}
