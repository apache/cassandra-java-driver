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
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Thrown when the checksums in a server response don't match (protocol v5 or above).
 *
 * <p>This indicates a data corruption issue, either due to a hardware issue on the client, or on
 * the network between the server and the client. It is not recoverable: the driver will drop the
 * connection.
 */
public class CrcMismatchException extends DriverException {

  public CrcMismatchException(@NonNull String message) {
    super(message, null, null, true);
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new CrcMismatchException(getMessage());
  }
}
