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
package com.datastax.dse.driver.api.core.config;

import com.datastax.oss.driver.api.core.config.DriverOption;
import edu.umd.cs.findbugs.annotations.NonNull;

public enum DseDriverOption implements DriverOption {
  /**
   * The name of the application using the session.
   *
   * <p>Value type: {@link String}
   */
  APPLICATION_NAME("basic.application.name"),
  /**
   * The version of the application using the session.
   *
   * <p>Value type: {@link String}
   */
  APPLICATION_VERSION("basic.application.version"),

  /**
   * Proxy authentication for GSSAPI authentication: allows to login as another user or role.
   *
   * <p>Value type: {@link String}
   */
  AUTH_PROVIDER_AUTHORIZATION_ID("advanced.auth-provider.authorization-id"),
  /**
   * Service name for GSSAPI authentication.
   *
   * <p>Value type: {@link String}
   */
  AUTH_PROVIDER_SERVICE("advanced.auth-provider.service"),
  /**
   * Login configuration for GSSAPI authentication.
   *
   * <p>Value type: {@link java.util.Map Map}&#60;{@link String},{@link String}&#62;
   */
  AUTH_PROVIDER_LOGIN_CONFIGURATION("advanced.auth-provider.login-configuration"),
  /**
   * Internal SASL properties, if any, such as QOP, for GSSAPI authentication.
   *
   * <p>Value type: {@link java.util.Map Map}&#60;{@link String},{@link String}&#62;
   */
  AUTH_PROVIDER_SASL_PROPERTIES("advanced.auth-provider.sasl-properties"),

  /**
   * The page size for continuous paging.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_PAGE_SIZE("advanced.continuous-paging.page-size"),
  /**
   * Whether {@link #CONTINUOUS_PAGING_PAGE_SIZE} should be interpreted in number of rows or bytes.
   *
   * <p>Value type: boolean
   */
  CONTINUOUS_PAGING_PAGE_SIZE_BYTES("advanced.continuous-paging.page-size-in-bytes"),
  /**
   * The maximum number of continuous pages to return.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_PAGES("advanced.continuous-paging.max-pages"),
  /**
   * The maximum number of continuous pages per second.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND("advanced.continuous-paging.max-pages-per-second"),
  /**
   * The maximum number of continuous pages that can be stored in the local queue.
   *
   * <p>Value type: int
   */
  CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES("advanced.continuous-paging.max-enqueued-pages"),
  /**
   * How long to wait for the coordinator to send the first continuous page.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE("advanced.continuous-paging.timeout.first-page"),
  /**
   * How long to wait for the coordinator to send subsequent continuous pages.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES("advanced.continuous-paging.timeout.other-pages"),

  /**
   * The largest latency that we expect to record for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_HIGHEST(
      "advanced.metrics.session.continuous-cql-requests.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for
   * continuous requests.
   *
   * <p>Value-type: int
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_DIGITS(
      "advanced.metrics.session.continuous-cql-requests.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_INTERVAL(
      "advanced.metrics.session.continuous-cql-requests.refresh-interval"),

  /**
   * The read consistency level to use for graph statements.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_READ_CONSISTENCY_LEVEL("basic.graph.read-consistency-level"),
  /**
   * The write consistency level to use for graph statements.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_WRITE_CONSISTENCY_LEVEL("basic.graph.write-consistency-level"),
  /**
   * The traversal source to use for graph statements.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_TRAVERSAL_SOURCE("basic.graph.traversal-source"),
  /**
   * The sub-protocol the driver will use to communicate with DSE Graph, on top of the Cassandra
   * native protocol.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_SUB_PROTOCOL("advanced.graph.sub-protocol"),
  /**
   * Whether a script statement represents a system query.
   *
   * <p>Value type: boolean
   */
  GRAPH_IS_SYSTEM_QUERY("basic.graph.is-system-query"),
  /**
   * The name of the graph targeted by graph statements.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_NAME("basic.graph.name"),
  /**
   * How long the driver waits for a graph request to complete.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  GRAPH_TIMEOUT("basic.graph.timeout"),

  /**
   * Whether to send events for Insights monitoring.
   *
   * <p>Value type: boolean
   */
  MONITOR_REPORTING_ENABLED("advanced.monitor-reporting.enabled"),

  /**
   * Whether to enable paging for Graph queries.
   *
   * <p>Value type: {@link String}
   */
  GRAPH_PAGING_ENABLED("advanced.graph.paging-enabled"),

  /**
   * The page size for Graph continuous paging.
   *
   * <p>Value type: int
   */
  GRAPH_CONTINUOUS_PAGING_PAGE_SIZE("advanced.graph.paging-options.page-size"),

  /**
   * The maximum number of Graph continuous pages to return.
   *
   * <p>Value type: int
   */
  GRAPH_CONTINUOUS_PAGING_MAX_PAGES("advanced.graph.paging-options.max-pages"),
  /**
   * The maximum number of Graph continuous pages per second.
   *
   * <p>Value type: int
   */
  GRAPH_CONTINUOUS_PAGING_MAX_PAGES_PER_SECOND(
      "advanced.graph.paging-options.max-pages-per-second"),
  /**
   * The maximum number of Graph continuous pages that can be stored in the local queue.
   *
   * <p>Value type: int
   */
  GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES("advanced.graph.paging-options.max-enqueued-pages"),
  /**
   * The largest latency that we expect to record for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_GRAPH_REQUESTS_HIGHEST("advanced.metrics.session.graph-requests.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for graph
   * requests.
   *
   * <p>Value-type: int
   */
  METRICS_SESSION_GRAPH_REQUESTS_DIGITS(
      "advanced.metrics.session.graph-requests.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_GRAPH_REQUESTS_INTERVAL(
      "advanced.metrics.session.graph-requests.refresh-interval"),
  /**
   * The largest latency that we expect to record for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_GRAPH_MESSAGES_HIGHEST("advanced.metrics.node.graph-messages.highest-latency"),
  /**
   * The number of significant decimal digits to which internal structures will maintain for graph
   * requests.
   *
   * <p>Value-type: int
   */
  METRICS_NODE_GRAPH_MESSAGES_DIGITS("advanced.metrics.node.graph-messages.significant-digits"),
  /**
   * The interval at which percentile data is refreshed for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_GRAPH_MESSAGES_INTERVAL("advanced.metrics.node.graph-messages.refresh-interval"),

  /**
   * The shortest latency that we expect to record for continuous requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_LOWEST(
      "advanced.metrics.session.continuous-cql-requests.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  CONTINUOUS_PAGING_METRICS_SESSION_CQL_REQUESTS_SLO(
      "advanced.metrics.session.continuous-cql-requests.slo"),

  /**
   * The shortest latency that we expect to record for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_GRAPH_REQUESTS_LOWEST("advanced.metrics.session.graph-requests.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_SESSION_GRAPH_REQUESTS_SLO("advanced.metrics.session.graph-requests.slo"),

  /**
   * The shortest latency that we expect to record for graph requests.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_GRAPH_MESSAGES_LOWEST("advanced.metrics.node.graph-messages.lowest-latency"),
  /**
   * Optional service-level objectives to meet, as a list of latencies to track.
   *
   * <p>Value-type: {@link java.time.Duration Duration}
   */
  METRICS_NODE_GRAPH_MESSAGES_SLO("advanced.metrics.node.graph-messages.slo"),
  ;

  private final String path;

  DseDriverOption(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }
}
