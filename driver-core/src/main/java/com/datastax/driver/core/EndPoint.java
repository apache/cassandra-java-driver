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
package com.datastax.driver.core;

import java.net.InetSocketAddress;

/** Encapsulates the information needed by the driver to open connections to a node. */
public interface EndPoint {

  /**
   * Resolves this instance to a socket address.
   *
   * <p>This will be called each time the driver opens a new connection to the node. The returned
   * address cannot be null.
   */
  InetSocketAddress resolve();
}
