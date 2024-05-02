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
package com.datastax.oss.driver.api.core.auth;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Indicates an error during the authentication phase while connecting to a node.
 *
 * <p>The only time when this is returned directly to the client (wrapped in a {@link
 * AllNodesFailedException}) is at initialization. If it happens later when the driver is already
 * connected, it is just logged and the connection will be reattempted.
 */
public class AuthenticationException extends RuntimeException {
  private static final long serialVersionUID = 0;

  private final EndPoint endPoint;

  public AuthenticationException(@NonNull EndPoint endPoint, @NonNull String message) {
    this(endPoint, message, null);
  }

  public AuthenticationException(
      @NonNull EndPoint endPoint, @NonNull String message, @Nullable Throwable cause) {
    super(String.format("Authentication error on node %s: %s", endPoint, message), cause);
    this.endPoint = endPoint;
  }

  /** The address of the node that encountered the error. */
  @NonNull
  public EndPoint getEndPoint() {
    return endPoint;
  }
}
