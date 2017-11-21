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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.detach.Detachable;

/**
 * A version of the native protocol used by the driver to communicate with the server.
 *
 * <p>The only reason to model this as an interface (as opposed to an enum type) is to accommodate
 * for custom protocol extensions. If you're connecting to a standard Apache Cassandra cluster, all
 * {@code ProtocolVersion}s are {@code CoreProtocolVersion} instances.
 */
public interface ProtocolVersion {
  /** The default version used for {@link Detachable detached} objects. */
  // Implementation note: we can't use the ProtocolVersionRegistry here, this has to be a
  // compile-time constant.
  ProtocolVersion DEFAULT = CoreProtocolVersion.V4;

  /**
   * A numeric code that uniquely identifies the version (this is the code used in network frames).
   */
  int getCode();

  /** A string representation of the version. */
  String name();

  /**
   * Whether the protocol version is in a beta status.
   *
   * <p>Beta versions are intended for Cassandra development. They should be used in a regular
   * application, beta features may break at any point.
   */
  boolean isBeta();
}
