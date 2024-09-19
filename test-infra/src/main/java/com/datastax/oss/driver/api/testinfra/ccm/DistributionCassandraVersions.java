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
package com.datastax.oss.driver.api.testinfra.ccm;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedMap;
import java.util.HashMap;
import java.util.Map;

/** Defines mapping of various distributions to shipped Apache Cassandra version. */
public abstract class DistributionCassandraVersions {
  private static final Map<BackendType, ImmutableSortedMap<Version, Version>> mappings =
      new HashMap<>();

  static {
    {
      // DSE
      ImmutableSortedMap<Version, Version> dse =
          ImmutableSortedMap.of(
              Version.V1_0_0, CcmBridge.V2_1_19,
              Version.V5_0_0, CcmBridge.V3_0_15,
              CcmBridge.V5_1_0, CcmBridge.V3_10,
              CcmBridge.V6_0_0, CcmBridge.V4_0_0);
      mappings.put(BackendType.DSE, dse);
    }
    {
      // HCD
      ImmutableSortedMap<Version, Version> hcd =
          ImmutableSortedMap.of(Version.V1_0_0, CcmBridge.V4_0_11);
      mappings.put(BackendType.HCD, hcd);
    }
  }

  public static Version getCassandraVersion(BackendType type, Version version) {
    ImmutableSortedMap<Version, Version> mapping = mappings.get(type);
    if (mapping == null) {
      return null;
    }
    return mapping.floorEntry(version).getValue();
  }
}
