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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;

/** A default consistency level supported by the driver out of the box. */
public enum DefaultConsistencyLevel implements ConsistencyLevel {
  ANY(ProtocolConstants.ConsistencyLevel.ANY),
  ONE(ProtocolConstants.ConsistencyLevel.ONE),
  TWO(ProtocolConstants.ConsistencyLevel.TWO),
  THREE(ProtocolConstants.ConsistencyLevel.THREE),
  QUORUM(ProtocolConstants.ConsistencyLevel.QUORUM),
  ALL(ProtocolConstants.ConsistencyLevel.ALL),
  LOCAL_ONE(ProtocolConstants.ConsistencyLevel.LOCAL_ONE),
  LOCAL_QUORUM(ProtocolConstants.ConsistencyLevel.LOCAL_QUORUM),
  EACH_QUORUM(ProtocolConstants.ConsistencyLevel.EACH_QUORUM),

  SERIAL(ProtocolConstants.ConsistencyLevel.SERIAL),
  LOCAL_SERIAL(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL),
  ;
  // Note that, for the sake of convenience, we also expose shortcuts to these constants on the
  // ConsistencyLevel interface. If you add a new enum constant, remember to update the interface as
  // well.

  private final int protocolCode;

  DefaultConsistencyLevel(int protocolCode) {
    this.protocolCode = protocolCode;
  }

  @Override
  public int getProtocolCode() {
    return protocolCode;
  }

  @NonNull
  public static DefaultConsistencyLevel fromCode(int code) {
    DefaultConsistencyLevel level = BY_CODE.get(code);
    if (level == null) {
      throw new IllegalArgumentException("Unknown code: " + code);
    }
    return level;
  }

  @Override
  public boolean isDcLocal() {
    return this == LOCAL_ONE || this == LOCAL_QUORUM || this == LOCAL_SERIAL;
  }

  @Override
  public boolean isSerial() {
    return this == SERIAL || this == LOCAL_SERIAL;
  }

  private static final Map<Integer, DefaultConsistencyLevel> BY_CODE = mapByCode(values());

  private static Map<Integer, DefaultConsistencyLevel> mapByCode(DefaultConsistencyLevel[] levels) {
    ImmutableMap.Builder<Integer, DefaultConsistencyLevel> builder = ImmutableMap.builder();
    for (DefaultConsistencyLevel level : levels) {
      builder.put(level.protocolCode, level);
    }
    return builder.build();
  }
}
