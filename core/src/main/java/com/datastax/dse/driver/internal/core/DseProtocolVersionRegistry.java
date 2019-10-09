/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.CassandraProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolFeature;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class DseProtocolVersionRegistry extends CassandraProtocolVersionRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(DseProtocolVersionRegistry.class);
  @VisibleForTesting static final Version DSE_4_7_0 = Version.parse("4.7.0");
  @VisibleForTesting static final Version DSE_5_0_0 = Version.parse("5.0.0");
  @VisibleForTesting static final Version DSE_5_1_0 = Version.parse("5.1.0");
  @VisibleForTesting static final Version DSE_6_0_0 = Version.parse("6.0.0");

  private final String logPrefix;

  public DseProtocolVersionRegistry(String logPrefix) {
    super(logPrefix, DefaultProtocolVersion.values(), DseProtocolVersion.values());
    this.logPrefix = logPrefix;
  }

  @Override
  public ProtocolVersion highestCommon(Collection<Node> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      throw new IllegalArgumentException("Expected at least one node");
    }

    // Sadly we can't trust the Cassandra version reported by DSE to infer the maximum OSS protocol
    // supported. For example DSE 6 reports release_version 4.0-SNAPSHOT, but only supports OSS
    // protocol v4 (while Cassandra 4 will support v5). So there's no way to reuse the OSS algorithm
    // from the parent class, simply redo everything:

    Set<ProtocolVersion> candidates = new HashSet<>();
    candidates.addAll(allNonBeta(DefaultProtocolVersion.values()));
    candidates.addAll(allNonBeta(DseProtocolVersion.values()));

    for (Node node : nodes) {
      List<ProtocolVersion> toEliminate = Collections.emptyList();

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
              triedVersionsForHighestCommon());
        } else if (dseVersion.compareTo(DSE_5_0_0) < 0) {
          // DSE 4.7 or 4.8 (Cassandra 2.1): OSS protocol v3
          toEliminate =
              ImmutableList.of(
                  DefaultProtocolVersion.V4, DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2);
        } else if (dseVersion.compareTo(DSE_5_1_0) < 0) {
          // DSE 5.0 (Cassandra 3): OSS protocol v4
          toEliminate = ImmutableList.of(DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2);
        } else if (dseVersion.compareTo(DSE_6_0_0) < 0) {
          // DSE 5.1: DSE protocol v1 or OSS protocol v4
          toEliminate = ImmutableList.of(DseProtocolVersion.DSE_V2);
        } // else DSE 6: DSE protocol v2 or OSS protocol v4
      } else {
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
        if (cassandraVersion.compareTo(Version.V2_1_0) < 0) {
          throw new UnsupportedProtocolVersionException(
              node.getEndPoint(),
              String.format(
                  "Node %s reports Cassandra version %s, "
                      + "but the driver only supports 2.1.0 and above",
                  node.getEndPoint(), cassandraVersion),
              ImmutableList.of(DefaultProtocolVersion.V3, DefaultProtocolVersion.V4));
        }

        LOG.debug(
            "[{}] Node {} reports Cassandra version {}",
            logPrefix,
            node.getEndPoint(),
            cassandraVersion);

        if (cassandraVersion.compareTo(Version.V2_2_0) < 0) {
          toEliminate =
              ImmutableList.of(
                  DefaultProtocolVersion.V4, DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2);
        } else {
          toEliminate = ImmutableList.of(DseProtocolVersion.DSE_V1, DseProtocolVersion.DSE_V2);
        }
      }

      for (ProtocolVersion version : toEliminate) {
        if (candidates.remove(version)) {
          LOG.debug("[{}] Excluding protocol {}", logPrefix, version);
        }
      }
    }

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
          triedVersionsForHighestCommon());
    } else {
      return max;
    }
  }

  // Simply all non-beta versions, since this is the set we start from before filtering
  private static ImmutableList<ProtocolVersion> triedVersionsForHighestCommon() {
    return ImmutableList.<ProtocolVersion>builder()
        .addAll(allNonBeta(DefaultProtocolVersion.values()))
        .addAll(allNonBeta(DseProtocolVersion.values()))
        .build();
  }

  private static <T extends Enum<T> & ProtocolVersion> Collection<T> allNonBeta(T[] versions) {
    ImmutableList.Builder<T> result = ImmutableList.builder();
    for (T version : versions) {
      if (!version.isBeta()) {
        result.add(version);
      }
    }
    return result.build();
  }

  @Override
  public boolean supports(ProtocolVersion version, ProtocolFeature feature) {
    int code = version.getCode();
    if (DefaultProtocolFeature.UNSET_BOUND_VALUES.equals(feature)) {
      // All DSE versions and all OSS V4+
      return DefaultProtocolVersion.V4.getCode() <= code;
    } else if (DefaultProtocolFeature.PER_REQUEST_KEYSPACE.equals(feature)) {
      // Only DSE_V2+ and OSS V5+
      return (DefaultProtocolVersion.V5.getCode() <= code
              && code < DseProtocolVersion.DSE_V1.getCode())
          || DseProtocolVersion.DSE_V2.getCode() <= code;
    } else if (DseProtocolFeature.CONTINUOUS_PAGING.equals(feature)) {
      // All DSE versions
      return DseProtocolVersion.DSE_V1.getCode() <= code;
    } else {
      throw new IllegalArgumentException("Unhandled protocol feature: " + feature);
    }
  }
}
