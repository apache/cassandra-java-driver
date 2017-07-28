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
 * Thrown when an incoming or outgoing protocol frame exceeds the limit defined by {@code
 * protocol.max-frame-length} in the configuration.
 *
 * <p>This error is always rethrown directly to the client, without any retry attempt.
 */
public class FrameTooLongException extends DriverException {

  private final SocketAddress address;

  public FrameTooLongException(SocketAddress address, String message) {
    super(message, null, false);
    this.address = address;
  }

  /** The address of the node that encountered the error. */
  public SocketAddress getAddress() {
    return address;
  }

  @Override
  public DriverException copy() {
    return new FrameTooLongException(address, getMessage());
  }
}
