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
package com.datastax.oss.driver.api.core.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import javax.net.ssl.SSLEngine;

/**
 * Extension point to configure SSL based on the built-in JDK implementation.
 *
 * <p>Note that, for advanced use cases (such as bypassing the JDK in favor of another SSL
 * implementation), the driver's internal API provides a lower-level interface: {@link
 * com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory}.
 */
public interface SslEngineFactory extends AutoCloseable {
  /**
   * Creates a new SSL engine each time a connection is established.
   *
   * @param remoteEndpoint the remote endpoint we are connecting to (the address of the Cassandra
   *     node).
   */
  @NonNull
  SSLEngine newSslEngine(@NonNull EndPoint remoteEndpoint);
}
