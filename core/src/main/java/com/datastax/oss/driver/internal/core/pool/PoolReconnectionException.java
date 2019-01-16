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
package com.datastax.oss.driver.internal.core.pool;

import com.datastax.oss.driver.api.core.DriverException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

public class PoolReconnectionException extends DriverException {
  /** The individual error for each connection that was attempted. */
  List<Throwable> connectionErrors;

  /**
   * If the reconnection attempted to complete a pool that already had active channels, their count.
   */
  int getActiveChannels;

  public PoolReconnectionException(List<Throwable> errors, int activeChannels) {
    super(null, null, errors.get(0), true);
    this.connectionErrors = errors;
    this.getActiveChannels = activeChannels;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new PoolReconnectionException(getConnectionErrors(), getActiveChannels);
  }

  public int getGetActiveChannels() {
    return getActiveChannels;
  }

  public List<Throwable> getConnectionErrors() {
    return connectionErrors;
  }
}
