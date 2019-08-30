/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.PagingEnabledOptions;
import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphPagingSupportChecker {
  private static final Logger LOG = LoggerFactory.getLogger(GraphPagingSupportChecker.class);
  static final Version GRAPH_PAGING_MIN_DSE_VERSION = Version.parse("6.8.0");

  private volatile Boolean contextGraphPagingEnabled;

  // Graph paging is available if
  // 1) continuous paging is generally available and
  // 2) all hosts are running DSE 6.8+
  // The computation below will be done only once when the session is initialized; if other hosts
  // join the cluster later and are not running DSE 6.8, the user has to manually disable graph
  // paging.
  boolean isPagingEnabled(GraphStatement<?> graphStatement, InternalDriverContext context) {
    DriverExecutionProfile driverExecutionProfile =
        Conversions.resolveExecutionProfile(graphStatement, context);
    LOG.trace(
        "GRAPH_PAGING_ENABLED: {}",
        driverExecutionProfile.getString(DseDriverOption.GRAPH_PAGING_ENABLED));

    PagingEnabledOptions pagingEnabledOptions =
        PagingEnabledOptions.valueOf(
            driverExecutionProfile.getString(DseDriverOption.GRAPH_PAGING_ENABLED));
    if (pagingEnabledOptions == PagingEnabledOptions.DISABLED) {
      return false;
    } else if (pagingEnabledOptions == PagingEnabledOptions.ENABLED) {
      return true;
    } else {
      return isContextGraphPagingEnabled(context);
    }
  }

  private boolean isContextGraphPagingEnabled(InternalDriverContext context) {
    if (contextGraphPagingEnabled == null) {
      ProtocolVersion protocolVersion = context.getProtocolVersion();

      if (!context
          .getProtocolVersionRegistry()
          .supports(protocolVersion, DseProtocolFeature.CONTINUOUS_PAGING)) {
        contextGraphPagingEnabled = false;
        return contextGraphPagingEnabled;
      }

      Collection<Node> nodes = context.getMetadataManager().getMetadata().getNodes().values();

      for (Node node : nodes) {
        Version dseVersion = (Version) node.getExtras().get(DseNodeProperties.DSE_VERSION);
        if (dseVersion == null
            || dseVersion.compareTo(Objects.requireNonNull(GRAPH_PAGING_MIN_DSE_VERSION)) < 0) {
          contextGraphPagingEnabled = false;
          return contextGraphPagingEnabled;
        }
      }
      contextGraphPagingEnabled = true;
      return contextGraphPagingEnabled;
    } else {
      return contextGraphPagingEnabled;
    }
  }
}
