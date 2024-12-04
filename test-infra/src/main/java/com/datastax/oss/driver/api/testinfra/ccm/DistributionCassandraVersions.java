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
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/** Defines mapping of various distributions to shipped Apache Cassandra version. */
public abstract class DistributionCassandraVersions {
  private static final Map<BackendType, TreeMap<Version, Version>> mappings = new HashMap<>();

  static {
    {
      // DSE
      TreeMap<Version, Version> dse = new TreeMap<>();
      dse.put(Version.V1_0_0, CcmBridge.V2_1_19);
      dse.put(Version.V5_0_0, CcmBridge.V3_0_15);
      dse.put(CcmBridge.V5_1_0, CcmBridge.V3_10);
      dse.put(CcmBridge.V6_0_0, CcmBridge.V4_0_0);
      mappings.put(BackendType.DSE, dse);
    }
    {
      // HCD
      TreeMap<Version, Version> hcd = new TreeMap<>();
      hcd.put(Version.V1_0_0, CcmBridge.V4_0_11);
      mappings.put(BackendType.HCD, hcd);
    }
  }

  public static Version getCassandraVersion(BackendType type, Version version) {
    TreeMap<Version, Version> mapping = mappings.get(type);
    if (mapping == null) {
      return null;
    }
    return mapping.floorEntry(version).getValue();
  }
}
