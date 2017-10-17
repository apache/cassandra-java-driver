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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.session.Request;
import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tracing information for a query.
 *
 * <p>When {@link Request#isTracing() tracing} is enabled for a query, Cassandra generates rows in
 * the {@code sessions} and {@code events} table of the {@code system_traces} keyspace. This class
 * is a client-side representation of that information.
 */
public interface QueryTrace {

  UUID getTracingId();

  String getRequestType();

  /** The server-side duration of the query in microseconds. */
  int getDurationMicros();

  /** The IP of the node that coordinated the query. */
  InetAddress getCoordinator();

  /** The parameters attached to this trace. */
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
  List<TraceEvent> getEvents();
}
