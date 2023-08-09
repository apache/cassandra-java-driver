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

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in implementation of the protocol version registry, supports all Cassandra and DSE
 * versions.
 */
@ThreadSafe
public class DefaultProtocolVersionRegistry implements ProtocolVersionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultProtocolVersionRegistry.class);
  private static final List<ProtocolVersion> allVersions =
      ImmutableList.<ProtocolVersion>builder()
          .add(DefaultProtocolVersion.values())
          .add(DseProtocolVersion.values())
          .build();

  @VisibleForTesting
  static final Version DSE_4_7_0 = Objects.requireNonNull(Version.parse("4.7.0"));

  @VisibleForTesting
  static final Version DSE_5_0_0 = Objects.requireNonNull(Version.parse("5.0.0"));

  @VisibleForTesting
  static final Version DSE_5_1_0 = Objects.requireNonNull(Version.parse("5.1.0"));

  @VisibleForTesting
  static final Version DSE_6_0_0 = Objects.requireNonNull(Version.parse("6.0.0"));

  @VisibleForTesting
  static final Version DSE_7_0_0 = Objects.requireNonNull(Version.parse("7.0.0"));

  private final String logPrefix;

  public DefaultProtocolVersionRegistry(String logPrefix) {
    this.logPrefix = logPrefix;
  }

  @Override
  public ProtocolVersion fromName(String name) {
    try {
      return DefaultProtocolVersion.valueOf(name);
    } catch (IllegalArgumentException noOssVersion) {
      try {
        return DseProtocolVersion.valueOf(name);
      } catch (IllegalArgumentException noDseVersion) {
        throw new IllegalArgumentException("Unknown protocol version name: " + name);
      }
    }
  }

  @Override
  public ProtocolVersion highestNonBeta() {
    ProtocolVersion highest = allVersions.get(allVersions.size() - 1);
    if (!highest.isBeta()) {
      return highest;
    } else {
      return downgrade(highest)
          .orElseThrow(() -> new AssertionError("There should be at least one non-beta version"));
    }
  }

  @Override
  public Optional<ProtocolVersion> downgrade(ProtocolVersion version) {
    int index = allVersions.indexOf(version);
    if (index < 0) {
      // This method is called with a value obtained from fromName, so this should never happen
      throw new AssertionError(version + " is not a known version");
    } else if (index == 0) {
      return Optional.empty();
    } else {
      ProtocolVersion previousVersion = allVersions.get(index - 1);
      // Beta versions are skipped during negotiation
      return previousVersion.isBeta() ? downgrade(previousVersion) : Optional.of(previousVersion);
    }
  }

  @Override
  public ProtocolVersion highestCommon(Collection<Node> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one node");
    }

    // Start with all non-beta versions (beta versions are always forced, and we don't call this
    // method if the version was forced).
    Set<ProtocolVersion> candidates = new LinkedHashSet<>();
    for (ProtocolVersion version : allVersions) {
      if (!version.isBeta()) {
        candidates.add(version);
      }
    }
    // Keep an unfiltered copy in case we need to throw an exception below
    ImmutableList<ProtocolVersion> initialCandidates = ImmutableList.copyOf(candidates);

    // For each node, remove the versions it doesn't support
    for (Node node : nodes) {

      // We can't trust the Cassandra version reported by DSE to infer the maximum OSS protocol
      // supported. For example DSE 6 reports release_version 4.0-SNAPSHOT, but only supports OSS
      // protocol v4 (while Cassandra 4 will support v5). So we treat DSE separately.
      Version dseVersion = (Version) node.getExtras().get(DseNodeProperties.DSE_VERSION);
      if (dseVersion != null) {
        LOG.debug("[{}] Node {} reports DSE version {}", logPrefix, node.getEndPoint(), dseVersion);
        dseVersion = dseVersion.nextStable();
        if (dseVersion.compareTo(DSE_4_7_0) < 0) {
          throw new UnsupportedProtocolVersionException(
              node.getEndPoint(),
              String.format(
                  "Node %s reports DSE version %s, "
                      + "but the driver only supports 4.7.0 and above",
                  node.getEndPoint(), dseVersion),
              initialCandidates);
        } else if (dseVersion.compareTo(DSE_5_0_0) < 0) {
          // DSE 4.7.x, 4.8.x
          removeHigherThan(DefaultProtocolVersion.V3, null, candidates);
        } else if (dseVersion.compareTo(DSE_5_1_0) < 0) {
          // DSE 5.0
          removeHigherThan(DefaultProtocolVersion.V4, null, candidates);
        } else if (dseVersion.compareTo(DSE_6_0_0) < 0) {
          // DSE 5.1
          removeHigherThan(DefaultProtocolVersion.V4, DseProtocolVersion.DSE_V1, candidates);
        } else if (dseVersion.compareTo(DSE_7_0_0) < 0) {
          // DSE 6
          removeHigherThan(DefaultProtocolVersion.V4, DseProtocolVersion.DSE_V2, candidates);
        } else {
          // DSE 7.0
          removeHigherThan(DefaultProtocolVersion.V5, DseProtocolVersion.DSE_V2, candidates);
        }
      } else { // not DSE
        Version cassandraVersion = node.getCassandraVersion();
        if (cassandraVersion == null) {
          LOG.warn(
              "[{}] Node {} reports neither DSE version nor Cassandra version, "
                  + "ignoring it from optimal protocol version computation",
              logPrefix,
              node.getEndPoint());
          continue;
        }
        cassandraVersion = cassandraVersion.nextStable();
        LOG.debug(
            "[{}] Node {} reports Cassandra version {}",
            logPrefix,
            node.getEndPoint(),
            cassandraVersion);
        if (cassandraVersion.compareTo(Version.V2_1_0) < 0) {
          throw new UnsupportedProtocolVersionException(
              node.getEndPoint(),
              String.format(
                  "Node %s reports Cassandra version %s, "
                      + "but the driver only supports 2.1.0 and above",
                  node.getEndPoint(), cassandraVersion),
              ImmutableList.of(DefaultProtocolVersion.V3, DefaultProtocolVersion.V4));
        } else if (cassandraVersion.compareTo(Version.V2_2_0) < 0) {
          // 2.1.0
          removeHigherThan(DefaultProtocolVersion.V3, null, candidates);
        } else if (cassandraVersion.compareTo(Version.V4_0_0) < 0) {
          // 2.2, 3.x
          removeHigherThan(DefaultProtocolVersion.V4, null, candidates);
        } else {
          // 4.0
          removeHigherThan(DefaultProtocolVersion.V5, null, candidates);
        }
      }
    }

    // If we have versions left, return the highest one
    ProtocolVersion max = null;
    for (ProtocolVersion candidate : candidates) {
      if (max == null || max.getCode() < candidate.getCode()) {
        max = candidate;
      }
    }
    if (max == null) { // Note: with the current algorithm, this never happens
      throw new UnsupportedProtocolVersionException(
          null,
          String.format(
              "Could not determine a common protocol version, "
                  + "enable DEBUG logs for '%s' for more details",
              LOG.getName()),
          initialCandidates);
    } else {
      return max;
    }
  }

  // Removes all versions strictly higher than the given versions from candidates. A null
  // maxDseVersion means "remove all DSE versions".
  private void removeHigherThan(
      DefaultProtocolVersion maxOssVersion,
      DseProtocolVersion maxDseVersion,
      Set<ProtocolVersion> candidates) {
    for (DefaultProtocolVersion ossVersion : DefaultProtocolVersion.values()) {
      if (ossVersion.compareTo(maxOssVersion) > 0 && candidates.remove(ossVersion)) {
        LOG.debug("[{}] Excluding protocol {}", logPrefix, ossVersion);
      }
    }
    for (DseProtocolVersion dseVersion : DseProtocolVersion.values()) {
      if ((maxDseVersion == null || dseVersion.compareTo(maxDseVersion) > 0)
          && candidates.remove(dseVersion)) {
        LOG.debug("[{}] Excluding protocol {}", logPrefix, dseVersion);
      }
    }
  }

  @Override
  public boolean supports(ProtocolVersion version, ProtocolFeature feature) {
    int code = version.getCode();
    if (DefaultProtocolFeature.SMALLINT_AND_TINYINT_TYPES.equals(feature)
        || DefaultProtocolFeature.DATE_TYPE.equals(feature)
        || DefaultProtocolFeature.UNSET_BOUND_VALUES.equals(feature)) {
      // All DSE versions and all OSS V4+
      return DefaultProtocolVersion.V4.getCode() <= code;
    } else if (DefaultProtocolFeature.PER_REQUEST_KEYSPACE.equals(feature)) {
      // Only DSE_V2+ and OSS V5+
      return (DefaultProtocolVersion.V5.getCode() <= code
              && code < DseProtocolVersion.DSE_V1.getCode())
          || DseProtocolVersion.DSE_V2.getCode() <= code;
    } else if (DefaultProtocolFeature.NOW_IN_SECONDS.equals(feature)
        || DefaultProtocolFeature.MODERN_FRAMING.equals(feature)) {
      // OSS only, V5+
      return DefaultProtocolVersion.V5.getCode() <= code
          && code < DseProtocolVersion.DSE_V1.getCode();
    } else if (DseProtocolFeature.CONTINUOUS_PAGING.equals(feature)) {
      // All DSE versions
      return DseProtocolVersion.DSE_V1.getCode() <= code;
    } else {
      throw new IllegalArgumentException("Unhandled protocol feature: " + feature);
    }
  }
}
