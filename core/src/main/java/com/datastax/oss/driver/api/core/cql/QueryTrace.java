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
package com.datastax.oss.driver.api.core.cql;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tracing information for a query.
 *
 * <p>When {@link Statement#isTracing() tracing} is enabled for a query, Cassandra generates rows in
 * the {@code sessions} and {@code events} table of the {@code system_traces} keyspace. This class
 * is a client-side representation of that information.
 */
public interface QueryTrace {

  @NonNull
  UUID getTracingId();

  @NonNull
  String getRequestType();

  /** The server-side duration of the query in microseconds. */
  int getDurationMicros();

  /**
   * @deprecated returns the coordinator IP, but {@link #getCoordinatorAddress()} should be
   *     preferred, since C* 4.0 and above now returns the port was well.
   */
  @NonNull
  @Deprecated
  InetAddress getCoordinator();

  /**
   * The IP and port of the node that coordinated the query. Prior to C* 4.0 the port is not set and
   * will default to 0.
   *
   * <p>This method's default implementation returns {@link #getCoordinator()} with the port set to
   * 0. The only reason it exists is to preserve binary compatibility. Internally, the driver
   * overrides it to set the correct port.
   *
   * @since 4.6.0
   */
  @NonNull
  default InetSocketAddress getCoordinatorAddress() {
    return new InetSocketAddress(getCoordinator(), 0);
  }

  /** The parameters attached to this trace. */
  @NonNull
  Map<String, String> getParameters();

  /** The server-side timestamp of the start of this query. */
  long getStartedAt();

  /**
   * The events contained in this trace.
   *
   * <p>Query tracing is asynchronous in Cassandra. Hence, it is possible for the list returned to
   * be missing some events for some of the replicas involved in the query if the query trace is
   * requested just after the return of the query (the only guarantee being that the list will
   * contain the events pertaining to the coordinator).
   */
  @NonNull
  List<TraceEvent> getEvents();
}
