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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultQueryTrace implements QueryTrace {

  private final UUID tracingId;
  private final String requestType;
  private final int durationMicros;
  private final InetSocketAddress coordinator;
  private final Map<String, String> parameters;
  private final long startedAt;
  private final List<TraceEvent> events;

  public DefaultQueryTrace(
      UUID tracingId,
      String requestType,
      int durationMicros,
      InetSocketAddress coordinator,
      Map<String, String> parameters,
      long startedAt,
      List<TraceEvent> events) {
    this.tracingId = tracingId;
    this.requestType = requestType;
    this.durationMicros = durationMicros;
    this.coordinator = coordinator;
    this.parameters = parameters;
    this.startedAt = startedAt;
    this.events = events;
  }

  @NonNull
  @Override
  public UUID getTracingId() {
    return tracingId;
  }

  @NonNull
  @Override
  public String getRequestType() {
    return requestType;
  }

  @Override
  public int getDurationMicros() {
    return durationMicros;
  }

  @NonNull
  @Override
  @Deprecated
  public InetAddress getCoordinator() {
    return coordinator.getAddress();
  }

  @NonNull
  @Override
  public InetSocketAddress getCoordinatorAddress() {
    return coordinator;
  }

  @NonNull
  @Override
  public Map<String, String> getParameters() {
    return parameters;
  }

  @Override
  public long getStartedAt() {
    return startedAt;
  }

  @NonNull
  @Override
  public List<TraceEvent> getEvents() {
    return events;
  }

  @Override
  public String toString() {
    return String.format("%s [%s] - %dµs", requestType, tracingId, durationMicros);
  }
}
