/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

/** Manages all the native protocol versions supported by the driver. */
public class ProtocolVersionRegistry {
  private final NavigableMap<Integer, ProtocolVersion> versionsByCode;

  public ProtocolVersionRegistry(ProtocolVersion[]... versionRanges) {
    this.versionsByCode = byCode(versionRanges);
  }

  /** Default implementation, initialized with the core OSS versions. */
  public ProtocolVersionRegistry() {
    this(CoreProtocolVersion.values());
  }

  public ProtocolVersion fromCode(int code) {
    ProtocolVersion protocolVersion = versionsByCode.get(code);
    if (protocolVersion == null) {
      throw new IllegalArgumentException("Unknown protocol version code: " + code);
    }
    return protocolVersion;
  }

  public ProtocolVersion fromName(String name) {
    for (ProtocolVersion version : versionsByCode.values()) {
      if (version.name().equals(name)) {
        return version;
      }
    }
    throw new IllegalArgumentException("Unknown protocol version name: " + name);
  }

  public ProtocolVersion highestNonBeta() {
    ProtocolVersion highest = versionsByCode.lastEntry().getValue();
    if (!highest.isBeta()) {
      return highest;
    } else {
      return downgrade(highest)
          .orElseThrow(() -> new AssertionError("There should be at least one non-beta version"));
    }
  }

  /**
   * Downgrade to a lower version if the current version is not supported by the server. This is
   * used during the protocol negotiation process.
   *
   * @return an empty optional if there is no version to downgrade to.
   */
  public Optional<ProtocolVersion> downgrade(ProtocolVersion version) {
    Map.Entry<Integer, ProtocolVersion> previousEntry =
        versionsByCode.lowerEntry(version.getCode());
    if (previousEntry == null) {
      return Optional.empty();
    } else {
      ProtocolVersion previousVersion = previousEntry.getValue();
      // Beta versions are skipped during negotiation
      return (previousVersion.isBeta()) ? downgrade(previousVersion) : Optional.of(previousVersion);
    }
  }

  private NavigableMap<Integer, ProtocolVersion> byCode(ProtocolVersion[][] versionRanges) {
    NavigableMap<Integer, ProtocolVersion> map = new TreeMap<>();
    for (ProtocolVersion[] versionRange : versionRanges) {
      for (ProtocolVersion version : versionRange) {
        ProtocolVersion previous = map.put(version.getCode(), version);
        Preconditions.checkArgument(
            previous == null,
            "Duplicate version code: %s in %s and %s",
            version.getCode(),
            previous,
            version);
      }
    }
    return map;
  }
}
