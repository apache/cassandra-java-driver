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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphSupportChecker {

  private static final Logger LOG = LoggerFactory.getLogger(GraphSupportChecker.class);

  /**
   * The minimum DSE version supporting both graph paging and the GraphBinary sub-protocol is DSE
   * 6.8.
   */
  private static final Version MIN_DSE_VERSION_GRAPH_BINARY_AND_PAGING =
      Objects.requireNonNull(Version.parse("6.8.0"));

  private volatile Boolean contextGraphPagingEnabled;
  private volatile Boolean isDse68OrAbove;

  /**
   * Checks whether graph paging is available.
   *
   * <p>Graph paging is available if:
   *
   * <ol>
   *   <li>Continuous paging is generally available (this implies protocol version {@link
   *       com.datastax.dse.driver.api.core.DseProtocolVersion#DSE_V1 DSE_V1} or higher);
   *   <li>Graph paging is set to <code>ENABLED</code> or <code>AUTO</code> in the configuration
   *       with {@link DseDriverOption#GRAPH_PAGING_ENABLED};
   *   <li>If graph paging is set to <code>AUTO</code>, then a check will be performed to verify
   *       that all hosts are running DSE 6.8+; if that is the case, then graph paging will be
   *       assumed to be available.
   * </ol>
   *
   * Note that the hosts check will be done only once, then memoized; if other hosts join the
   * cluster later and do not support graph paging, the user has to manually disable graph paging.
   */
  public boolean isPagingEnabled(
      @NonNull GraphStatement<?> graphStatement, @NonNull InternalDriverContext context) {
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

  /**
   * Infers the {@link GraphProtocol} to use to execute the given statement.
   *
   * <p>The graph protocol is computed as follows:
   *
   * <ol>
   *   <li>If the statement declares the protocol to use with {@link
   *       GraphStatement#getSubProtocol()}, then that protocol is returned.
   *   <li>If the driver configuration explicitly defines the protocol to use (see {@link
   *       DseDriverOption#GRAPH_SUB_PROTOCOL} and reference.conf), then that protocol is returned.
   *   <li>Otherwise, the graph protocol to use is determined by the DSE version of hosts in the
   *       cluster. If any host has DSE version 6.7.x or lower, the default graph protocol is {@link
   *       GraphProtocol#GRAPHSON_2_0}. If all hosts have DSE version 6.8.0 or higher, the default
   *       graph protocol is {@link GraphProtocol#GRAPH_BINARY_1_0}.
   * </ol>
   *
   * Note that the hosts check will be done only once, then memoized; if other hosts join the and do
   * not support the computed graph protocol, the user has to manually set the graph protocol to
   * use.
   *
   * <p>Also note that <code>GRAPH_BINARY_1_0</code> can only be used with "core" graph engines; if
   * you are targeting a "classic" graph engine instead, the user has to manually set the graph
   * protocol to something else.
   */
  @NonNull
  public GraphProtocol inferGraphProtocol(
      @NonNull GraphStatement<?> statement,
      @NonNull DriverExecutionProfile config,
      @NonNull InternalDriverContext context) {
    String graphProtocol = statement.getSubProtocol();
    if (graphProtocol == null) {
      // use the protocol specified in configuration, otherwise get the default from the context
      graphProtocol =
          config.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL)
              ? config.getString(DseDriverOption.GRAPH_SUB_PROTOCOL)
              : getDefaultGraphProtocol(context).toInternalCode();
    }
    // should not be null because we call config.getString() with a default value
    Objects.requireNonNull(
        graphProtocol,
        "Could not determine the graph protocol for the query. This is a bug, please report.");

    return GraphProtocol.fromString(graphProtocol);
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
   * Determines the default {@link GraphProtocol} for the given context.
   *
   * @return The default GraphProtocol to used based on the provided context.
   */
  @VisibleForTesting
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
}
