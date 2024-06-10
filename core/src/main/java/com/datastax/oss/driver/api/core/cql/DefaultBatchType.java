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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.protocol.internal.ProtocolConstants;

/** A default batch type supported by the driver out of the box. */
public enum DefaultBatchType implements BatchType {
  /**
   * A logged batch: Cassandra will first write the batch to its distributed batch log to ensure the
   * atomicity of the batch (atomicity meaning that if any statement in the batch succeeds, all will
   * eventually succeed).
   */
  LOGGED(ProtocolConstants.BatchType.LOGGED),

  /**
   * A batch that doesn't use Cassandra's distributed batch log. Such batch are not guaranteed to be
   * atomic.
   */
  UNLOGGED(ProtocolConstants.BatchType.UNLOGGED),

  /**
   * A counter batch. Note that such batch is the only type that can contain counter operations and
   * it can only contain these.
   */
  COUNTER(ProtocolConstants.BatchType.COUNTER),
  ;
  // Note that, for the sake of convenience, we also expose shortcuts to these constants on the
  // BatchType interface. If you add a new enum constant, remember to update the interface as
  // well.

  private final byte code;

  DefaultBatchType(byte code) {
    this.code = code;
  }

  @Override
  public byte getProtocolCode() {
    return code;
  }
}
