/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.connection;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.SocketAddress;

/**
 * Thrown when an incoming or outgoing protocol frame exceeds the limit defined by {@code
 * protocol.max-frame-length} in the configuration.
 *
 * <p>This error is always rethrown directly to the client, without any retry attempt.
 */
public class FrameTooLongException extends DriverException {

  private final SocketAddress address;

  public FrameTooLongException(@NonNull SocketAddress address, @NonNull String message) {
    this(address, message, null);
  }

  private FrameTooLongException(
      SocketAddress address, String message, ExecutionInfo executionInfo) {
    super(message, executionInfo, null, false);
    this.address = address;
  }

  /** The address of the node that encountered the error. */
  @NonNull
  public SocketAddress getAddress() {
    return address;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new FrameTooLongException(address, getMessage(), getExecutionInfo());
  }
}
