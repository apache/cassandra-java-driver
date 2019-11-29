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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphSupportChecker {
  private static final Logger LOG = LoggerFactory.getLogger(GraphSupportChecker.class);
  static final Version MIN_DSE_VERSION_GRAPH_BINARY_AND_PAGING =
      Objects.requireNonNull(Version.parse("6.8.0"));

  private volatile Boolean contextGraphPagingEnabled;
  private volatile Boolean isDse68OrAbove;

  // Graph paging is available if
  // 1) continuous paging is generally available and
  // 2) all hosts are running DSE 6.8+
  // The computation below will be done only once when the session is initialized; if other hosts
  // join the cluster later and are not running DSE 6.8, the user has to manually disable graph
  // paging.
  boolean isPagingEnabled(GraphStatement<?> graphStatement, InternalDriverContext context) {
    DriverExecutionProfile driverExecutionProfile =
        Conversions.resolveExecutionProfile(graphStatement, context);
    PagingEnabledOptions pagingEnabledOptions =
        PagingEnabledOptions.valueOf(
            driverExecutionProfile.getString(DseDriverOption.GRAPH_PAGING_ENABLED));
    if (LOG.isTraceEnabled()) {
      LOG.trace("GRAPH_PAGING_ENABLED: {}", pagingEnabledOptions);
    }
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
      } else {
        if (isDse68OrAbove == null) {
          isDse68OrAbove = checkIsDse68OrAbove(context);
        }
        contextGraphPagingEnabled = isDse68OrAbove;
      }
    }
    return contextGraphPagingEnabled;
  }

  /**
   * Determines the default {@link GraphProtocol} for the given context. When a statement is
   * executed, if the Graph protocol is not explicitly set on the statement (via {@link
   * GraphStatement#setSubProtocol(java.lang.String)}), or is not explicitly set in the config (see
   * dse-reference.conf), the default Graph protocol used is determined by the DSE version to which
   * the driver is connected. For DSE versions 6.7.x and lower, the default Graph protocol is {@link
   * GraphProtocol#GRAPHSON_2_0}. For DSE versions 6.8.0 and higher, the default Graph protocol is
   * {@link GraphProtocol#GRAPH_BINARY_1_0}.
   *
   * @return The default GraphProtocol to used based on the provided context.
   */
  GraphProtocol getDefaultGraphProtocol(@NonNull InternalDriverContext context) {
    if (isDse68OrAbove == null) {
      isDse68OrAbove = checkIsDse68OrAbove(context);
    }
    // if the DSE version can't be determined, default to GraphSON 2.0
    return isDse68OrAbove ? GraphProtocol.GRAPH_BINARY_1_0 : GraphProtocol.GRAPHSON_2_0;
  }

  private boolean checkIsDse68OrAbove(@NonNull InternalDriverContext context) {
    Collection<Node> nodes = context.getMetadataManager().getMetadata().getNodes().values();

    for (Node node : nodes) {
      Version dseVersion = (Version) node.getExtras().get(DseNodeProperties.DSE_VERSION);
      if (dseVersion == null || dseVersion.compareTo(MIN_DSE_VERSION_GRAPH_BINARY_AND_PAGING) < 0) {
        return false;
      }
    }
    return true;
  }

  GraphProtocol inferGraphProtocol(
      GraphStatement<?> statement, DriverExecutionProfile config, InternalDriverContext context) {
    String graphProtocol = statement.getSubProtocol();
    if (graphProtocol == null) {
      // use the protocol specified in configuration, otherwise get the default from the context
      graphProtocol =
          (config.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL))
              ? config.getString(DseDriverOption.GRAPH_SUB_PROTOCOL)
              : getDefaultGraphProtocol(context).toInternalCode();
    }
    // should not be null because we call config.getString() with a default value
    Objects.requireNonNull(
        graphProtocol,
        "Could not determine the graph protocol for the query. This is a bug, please report.");

    return GraphProtocol.fromString(graphProtocol);
  }
}
