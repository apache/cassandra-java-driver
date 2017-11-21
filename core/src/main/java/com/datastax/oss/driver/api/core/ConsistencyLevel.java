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

import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

/** The consistency level of a request. */
public enum ConsistencyLevel {
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

  private final int protocolCode;

  ConsistencyLevel(int protocolCode) {
    this.protocolCode = protocolCode;
  }

  public int getProtocolCode() {
    return protocolCode;
  }

  public static ConsistencyLevel fromCode(int code) {
    ConsistencyLevel level = BY_CODE.get(code);
    if (level == null) {
      throw new IllegalArgumentException("Unknown code: " + code);
    }
    return level;
  }

  private static Map<Integer, ConsistencyLevel> BY_CODE = mapByCode(values());

  private static Map<Integer, ConsistencyLevel> mapByCode(ConsistencyLevel[] levels) {
    ImmutableMap.Builder<Integer, ConsistencyLevel> builder = ImmutableMap.builder();
    for (ConsistencyLevel level : levels) {
      builder.put(level.protocolCode, level);
    }
    return builder.build();
  }
}
