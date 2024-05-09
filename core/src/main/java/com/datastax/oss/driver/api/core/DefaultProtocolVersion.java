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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.protocol.internal.ProtocolConstants;

/**
 * A protocol version supported by default by the driver.
 *
 * <p>Legacy versions 1 (Cassandra 1.2) and 2 (Cassandra 2.0) are not supported anymore.
 */
public enum DefaultProtocolVersion implements ProtocolVersion {

  /** Version 3, supported by Cassandra 2.1 and above. */
  V3(ProtocolConstants.Version.V3, false),

  /** Version 4, supported by Cassandra 2.2 and above. */
  V4(ProtocolConstants.Version.V4, false),

  /** Version 5, supported by Cassandra 4.0 and above. */
  V5(ProtocolConstants.Version.V5, false),

  /**
   * Version 6, currently supported as a beta preview in Cassandra 4.0 and above.
   *
   * <p>Do not use this in production.
   *
   * @see ProtocolVersion#isBeta()
   */
  V6(ProtocolConstants.Version.V6, true),
  ;
  // Note that, for the sake of convenience, we also expose shortcuts to these constants on the
  // ProtocolVersion interface. If you add a new enum constant, remember to update the interface as
  // well.

  private final int code;
  private final boolean beta;

  DefaultProtocolVersion(int code, boolean beta) {
    this.code = code;
    this.beta = beta;
  }

  @Override
  public int getCode() {
    return code;
  }

  @Override
  public boolean isBeta() {
    return beta;
  }
}
