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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in implementation of the protocol version registry, that supports the protocol versions of
 * Apache Cassandra.
 *
 * <p>This can be overridden with a custom implementation by subclassing {@link
 * DefaultDriverContext}.
 *
 * @see DefaultProtocolVersion
 */
@ThreadSafe
public class CassandraProtocolVersionRegistry implements ProtocolVersionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraProtocolVersionRegistry.class);
  private static final ImmutableList<ProtocolVersion> values =
      ImmutableList.<ProtocolVersion>builder().add(DefaultProtocolVersion.values()).build();

  private final String logPrefix;
  private final NavigableMap<Integer, ProtocolVersion> versionsByCode;

  public CassandraProtocolVersionRegistry(String logPrefix) {
    this(logPrefix, DefaultProtocolVersion.values());
  }

  protected CassandraProtocolVersionRegistry(String logPrefix, ProtocolVersion[]... versionRanges) {
    this.logPrefix = logPrefix;
    this.versionsByCode = byCode(versionRanges);
  }

  @Override
  public ProtocolVersion fromCode(int code) {
    ProtocolVersion protocolVersion = versionsByCode.get(code);
    if (protocolVersion == null) {
      throw new IllegalArgumentException("Unknown protocol version code: " + code);
    }
    return protocolVersion;
  }

  @Override
  public ProtocolVersion fromName(String name) {
    for (ProtocolVersion version : versionsByCode.values()) {
      if (version.name().equals(name)) {
        return version;
      }
    }
    throw new IllegalArgumentException("Unknown protocol version name: " + name);
  }

  @Override
  public ProtocolVersion highestNonBeta() {
    ProtocolVersion highest = versionsByCode.lastEntry().getValue();
    if (!highest.isBeta()) {
      return highest;
    } else {
      return downgrade(highest)
          .orElseThrow(() -> new AssertionError("There should be at least one non-beta version"));
    }
  }

  @Override
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

  @Override
  public ProtocolVersion highestCommon(Collection<Node> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one node");
    }

    SortedSet<DefaultProtocolVersion> candidates = new TreeSet<>();

    for (DefaultProtocolVersion version : DefaultProtocolVersion.values()) {
      // Beta versions always need to be forced, and we only call this method if the version
      // wasn't forced
      if (!version.isBeta()) {
        candidates.add(version);
      }
    }

    // The C*<=>protocol mapping is hardcoded in the code below, I don't see a need to be more
    // sophisticated right now.
    for (Node node : nodes) {
      Version version = node.getCassandraVersion();
      if (version == null) {
        LOG.warn(
            "[{}] Node {} reports null Cassandra version, "
                + "ignoring it from optimal protocol version computation",
            logPrefix,
            node.getEndPoint());
        continue;
      }
      version = version.nextStable();
      if (version.compareTo(Version.V2_1_0) < 0) {
        throw new UnsupportedProtocolVersionException(
            node.getEndPoint(),
            String.format(
                "Node %s reports Cassandra version %s, "
                    + "but the driver only supports 2.1.0 and above",
                node.getEndPoint(), version),
            ImmutableList.of(DefaultProtocolVersion.V3, DefaultProtocolVersion.V4));
      }

      LOG.debug(
          "[{}] Node {} reports Cassandra version {}", logPrefix, node.getEndPoint(), version);
      if (version.compareTo(Version.V2_2_0) < 0 && candidates.remove(DefaultProtocolVersion.V4)) {
        LOG.debug("[{}] Excluding protocol V4", logPrefix);
      }
    }

    if (candidates.isEmpty()) {
      // Note: with the current algorithm, this never happens
      throw new UnsupportedProtocolVersionException(
          null,
          String.format(
              "Could not determine a common protocol version, "
                  + "enable DEBUG logs for '%s' for more details",
              LOG.getName()),
          ImmutableList.of(DefaultProtocolVersion.V3, DefaultProtocolVersion.V4));
    } else {
      return candidates.last();
    }
  }

  @Override
  public boolean supports(ProtocolVersion version, ProtocolFeature feature) {
    if (DefaultProtocolFeature.UNSET_BOUND_VALUES.equals(feature)) {
      return version.getCode() >= 4;
    } else if (DefaultProtocolFeature.PER_REQUEST_KEYSPACE.equals(feature)) {
      return version.getCode() >= 5;
    } else {
      throw new IllegalArgumentException("Unhandled protocol feature: " + feature);
    }
  }

  @Override
  public ImmutableList<ProtocolVersion> getValues() {
    return values;
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
